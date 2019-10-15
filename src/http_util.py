import json
import pycurl
#import StringIO
import io
import json

def curl_http_url(urlstr):
  #response = StringIO.StringIO()
  #response = io.StringIO()
  response = io.BytesIO()
  c = pycurl.Curl()
  c.setopt(c.URL, str(urlstr))
  c.setopt(c.WRITEFUNCTION, response.write)
  c.setopt(c.HTTPHEADER, ['Content-Type: application/json','Accept-Charset: UTF-8'])
  #c.setopt(c.POSTFIELDS, '@request.json')
  c.perform()
  c.close()

  rst = response.getvalue()
  response.close()

  res_dict = json.loads(rst)
  if res_dict['status'] == 0:
    #return json.dumps(res_dict['data'])
    return res_dict['data']
  else:
    return None
