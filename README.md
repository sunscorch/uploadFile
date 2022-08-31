# uploadFile
a simple py program to use databricks rest api to upload large files > 1 MB </br>
based on the doc：　https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/dbfs</br>
databricks native rest api does not support we upload a large file >=1MB.</br>
This program can help you combine create , add_block and close API in above doc ,which can help us upload large files.</br>
other alternative is databricks cli to upload large file.

**usage:**
```
test = LoadFile("<workspace_url>", "<PAT token>")
test.upload_file("<source_data_file_path>", "<dbfs_file_path>")
```
