import os
import sys
from datetime import datetime, timedelta

bucket_name = sys.argv[1]
if len(sys.argv) > 2:
    minus = int(sys.argv[2])
else:
    minus = 0

today = (datetime.today() - timedelta(days=minus)).strftime("%Y%m%d")

os.popen('wget -O data.csv https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-province/dpc-covid19-ita-province-%s.csv' % today).read()
os.popen('gsutil mv data.csv gs://%s/dpc-covid19-ita-province-%s.csv' % (bucket_name, today)).read()
