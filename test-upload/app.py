# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import tempfile

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

client = storage.Client()

bucket = client.bucket("example-spark")
blob = bucket.blob("input/test-upload-1.csv")

blob.upload_from_string("upload-test-1")

# List the Buckets
# for bucket in client.list_buckets():
#     print(f"Bucket: {bucket.name}\n")

#     # List the Blobs in each Bucket
#     for blob in bucket.list_blobs():
#         print(f"Blob: {blob.name}")

#         # Print the content of the Blob
#         b = bucket.get_blob(blob.name)
#         with tempfile.NamedTemporaryFile() as temp_file:
#             s = b.download_to_filename(temp_file.name)
#             temp_file.seek(0, 0)
#             print(temp_file.read(), "\n")
