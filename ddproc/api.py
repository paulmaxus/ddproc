import re
import json
import zipfile
import pandas as pd

from pathlib import Path
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient


class Config(dict):
    """
    Configuration class that extends dict to allow attribute-style access to configuration settings.

    Example:
        config = Config(account="my_account")
        print(config.account)  # Accessed as attribute
        print(config["account"])  # Accessed as dictionary
    """

    def __getattr__(self, key):
        return super().__getitem__(key)

    def __setattr__(self, key, value):
        return super().__setitem__(key, value)


config = Config(
    azure_account = "my_account",
    azure_container = "my_container",
    data_folder = "."
)


class Processor:
    def __init__(self, specs=None, replacement_file=None):
        self.specs = {
            "youtube": {
                "schema": ".*participant-(?P<id>\d+\w?)_source-YouTube_key-\w+.json",
                "processor": self._extract_youtube
            },
            "tiktok": {
                "schema": ".*participant-(?P<id>\d+\w?)_source-TikTok_key-\w+.json",
                "processor": self._extract_tiktok,
            },
            "youtube-questionnaire": {
                "schema": ".*participant-(?P<id>\d+\w?)_source-YouTube_key-(?P<timestamp>\d+)-questionnaire-donation.json",
                "processor": self._extract_youtube_questionnaire
            },
            "tiktok-questionnaire": {
                "schema": ".*participant-(?P<id>\d+\w?)_source-TikTok_key-(?P<timestamp>\d+)-questionnaire-donation.json",
                "processor": self._extract_tiktok_questionnaire
            }
        } if specs is None else specs
        # compile schemas
        self.specs = {k: {**v, "regex": re.compile(v["schema"])} for k, v in self.specs.items()}

        self.replacement = pd.read_csv(replacement_file, dtype=object, index_col="id") if replacement_file else None

        self.metadata = self.load()
        self.replace()
    
    def load(self):
        # Read filenames from zipfile and filter
        metadata = []
        with zipfile.ZipFile(Path(config.data_folder) / "data.zip", 'r') as zip_ref:
            for filename in zip_ref.namelist():
                for platform in self.specs:
                    regex = self.specs[platform]["regex"]
                    match = regex.match(filename)
                    if match:
                        m = match.groupdict()
                        m = {k.lower(): v.lower() for k, v in m.items()}
                        m["platform"] = platform
                        m["filename"] = match[0]
                        metadata.append(m)
                        break
        return metadata

    def replace(self):
        if self.replacement is None:
            return
        
        r = self.replacement
        new_metadata = []
        for m in self.metadata:
            # Is this participant replaced or a replacement?
            if m["id"] in r.index:
                # Replacement
                # If it doesn't replace anything we skip
                if not int(r.loc[m["id"], m["platform"]]):
                    print("skipping", 
                          m["id"][0] + "*"*len(m["id"][1:-1]) + m["id"][-1], 
                          "for", m["platform"])
                    continue
                else:
                    new_id = str(r.loc[m["id"], "replaces"])
                    new_m = m.copy()
                    new_m["id"] = new_id
                    print("replacing", 
                          new_id[0] + "*"*len(new_id[1:-1]) + new_id[-1],
                          "with", 
                          m["id"][0] + "*"*len(m["id"][1:-1]) + m["id"][-1],
                          "for", m["platform"])
            elif r[r.replaces==m["id"]][m["platform"]].values.astype(int).any():
                # If it is replaced by something we skip
                print("skipping", 
                      m["id"][0] + "*"*len(m["id"][1:-1]) + m["id"][-1], 
                      "for", m["platform"]
                )
            else:
                # We simply add it
                new_metadata.append(m)

        self.metadata = new_metadata
        
    def extract(self):
        # Extract and store data from data files
        dfs = {}
        with zipfile.ZipFile(Path(config.data_folder) / "data.zip", 'r') as zip_ref:
            for m in self.metadata:
                with zip_ref.open(m["filename"], 'r') as f:
                    data = json.loads(f.read())
                    tables = self.specs[m["platform"]]["processor"](data)
                    for dtype, table in tables:
                        table["id"] = m["id"]
                        if "timestamp" in m:
                            table["timestamp"] = m["timestamp"]
                        if dtype not in dfs:
                            dfs[dtype] = table
                        else:
                            dfs[dtype] = pd.concat([dfs[dtype], table])
        return dfs
    

    def _extract_youtube(self, data):
        tables = []
        dtypes = ['youtube_watch_history', 'youtube_search_history', 'youtube_subscriptions']
        for d in data:
            for dtype in d:    
                if dtype in dtypes:
                    tables.append((dtype, pd.DataFrame(d[dtype])))
        return tables

    def _extract_tiktok(self, data):
        tables = []
        for dtype in data:
            new_dtype = dtype
            if "tiktok_video_browsing_history" in dtype:
                new_dtype = "tiktok_video_browsing_history"
            tables.append((new_dtype, pd.DataFrame(data[dtype])))
        return tables
    
    def _extract_youtube_questionnaire(self, data):
        tables = []
        tables.append(("youtube_questionnaire", 
                       pd.DataFrame(data.values(), columns=["answer"])))
        return tables
    
    def _extract_tiktok_questionnaire(self, data):
        tables = []
        tables.append(("tiktok_questionnaire", 
                       pd.DataFrame(data.values(), columns=["answer"])))
        return tables

def download_from_azure():
    url = f"https://{config.azure_account}.blob.core.windows.net"
    credentials = DefaultAzureCredential()
    try:
        container_client = ContainerClient(account_url=url, 
                                           container_name=config.azure_container, 
                                           credential=credentials)
        blob_list = container_client.list_blobs()

        with zipfile.ZipFile(Path(config.data_folder) / "data.zip", "w", zipfile.ZIP_DEFLATED) as zf:
            for blob in blob_list:
                blob_client = container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob()
                zf.writestr(blob.name, blob_data.readall())

    except Exception as e:
        print("Download failed:", e)
        raise e