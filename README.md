# aws-tree

Here is me storing a tree of (smallish) items in s3 and dynamoDB and comparison
of the item and entire tree retrieval.

Lessons to learn:

* dynamoDB wins hands down.
* async io works well, use it!

# s3tree

I have an S3 bucket with 110 'files' (with small text content) spread between
various directories.  My task is to retrieve all these and to produce a
unifying JSON.

Approach|Time (s)
--------|----
`aws s3 cp --recursive s3://bucket`|4.72
`s3tree_get.py bucket`|11.00
`s3tree_aget.py bucket`|2.36

# DynamoDB Tree
