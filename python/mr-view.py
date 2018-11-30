#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from couchbase.bucket import Bucket

# for more
# https://pythonhosted.org/couchbase/api/couchbase.html
# http://docs.couchbase.com/sdk-api/couchbase-python-client-2.0.0-beta2/api/couchbase.html                                                                  
bucket = Bucket("couchbase://127.0.0.1:8091/bbtest", password='123456')

design_document = 'bbtest'
view = 'bbtest'

view_results = bucket.query(design_document, view, limit=10)
count = 0
for result in view_results:
    print result.key
    count += 1
    assert count < 10
