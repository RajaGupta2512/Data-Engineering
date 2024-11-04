This is a python script that can run in AWS Lamda Function and move all the S3 objects/files from a Source folder to Target folder whenever new objects/files are uploaded into the SOurce folder.

Target folder will have a folder created with format (yyyymmddHHMMSS)+use_case_id, where use_case_id can be provided in the lamda function's configurations by the user.

For example: all files from source folder will be moved into a folder 20241104051305113, 114 = use_case-id
