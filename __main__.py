# Standard library imports
import json
import requests
import calendar
import time
import re
# Third party imports
from ibm_watson import DiscoveryV1, ApiException
from lxml import etree

#env variable for logging
env = ""

def get_article_body(text):
	dom = etree.HTML(text)
	if dom == None:
		return ""
	body_parses = []
	body_parses.append(dom.xpath('//p[contains(@class,"article")]//text()'))
	body_parses.append(dom.xpath('//p[contains(@class,"linkdb-marker")]//text()'))
	body_parses.append(dom.xpath('//div[(contains(@class,"article") or contains(@class,"body")or contains(@class,"Main") or contains(@class,"mntl-sc-page") or contains(@class,"content") or contains(@class,"listicle") or contains(@class,"SectionBreak")) and not(contains(@class,"fullbleed"))]/*[self::p or self::h2]//text()'))
	body_parses.append(dom.xpath('//div[(contains(@class,"article") or contains(@class,"body")or contains(@class,"Main") or contains(@class,"mntl-sc-page") or contains(@class,"content") or contains(@class,"listicle") or contains(@class,"SectionBreak")) and not(contains(@class,"fullbleed"))]/*/*[self::p or self::h2]//text()'))
	body_parses.append(dom.xpath('//div[(contains(@class,"article") or contains(@class,"body")or contains(@class,"Main") or contains(@class,"mntl-sc-page") or contains(@class,"content") or contains(@class,"listicle") or contains(@class,"SectionBreak")) and not(contains(@class,"fullbleed"))]/*/*/*[self::p or self::h2]//text()'))
	#body_parses.append(dom.xpath('//div[contains(@class,"description")]//text()'))
	body_parses.append(dom.xpath('//div[contains(@class,"section-text")]//text()'))
	body_parses.append(dom.xpath('//div[contains(@class,"cn-body")]/div//text()'))
	body_parses.append(dom.xpath('//div[(contains(@id,"Body") or contains(@id,"content") or contains(@class,"rich-text"))]/p//text()'))
	body_parses.append(dom.xpath('//article[(contains(@class,"content") or contains(@class,"article"))]//*[self::p or self::h2]//text()'))
	body_parses.append(dom.xpath('//article[not(contains(@class,"content") or contains(@class,"article"))]//*[self::p or self::h2]//text()'))
	#body_parses.append(dom.xpath('//text()'))
	
	js_json = dom.xpath('//script[contains(@type,"application/json")]//text()')
	js_text = ""
	if js_json:
		try:
			jsj = json.loads(js_json[0])
			for body in jsj["props"]["pageProps"]["articleData"]["body"]:
				if "content" in body:
					for content in body["content"]:
						if "text" in content:
							js_text = js_text + content["text"] + " "
							#print(content["text"])
		except Exception as ex:
			print("COULD NOT FIND JSON/JAVASCRIPT TEXT")
	body_parses.append([js_text])
	
	body = ""
	max_length = 0
	for bp in body_parses:
		if len(bp) > max_length:
			max_length = len(bp)
			body = bp
	body = re.sub(r'\xa0', '', " ".join(body))
	body = re.sub(r'[\n\r]+','',body)
	body = re.sub(r'[ ]{2,}',' ', body)
	
	return body
	
	
def translate_text(url, translate_apikey, language, text):

	if "en" in language or "unk" in language:
		return text
		
	language_mapping = {
		"ger": "DE",
		"de-DE": "DE",
		"it-IT": "IT",
		"fr-FR": "FR",
		"es-ES": "ES"
	}
	
	if language not in language_mapping:
		return text

	n=1500
	chunks = []
	i = 0
	while i+n < len(text):
		for k in range(i+n, len(text)):
			if text[k] == ".":
				k = k+1
				break
			if text[k] == ">":
				break
		chunks.append(text[i:k])
		i = k+1
	chunks.append(text[i:])
	
	output = ""
	for chunk in chunks:
		data = {
			"auth_key": translate_apikey,
			"text": chunk,
			"source_lang": language_mapping[language],
			"target_lang": "EN-US"
		}
		try:
			r = requests.get(url, params=data)
			
			r.raise_for_status()
			output = output + r.json()["translations"][0]["text"] + " "
		except Exception as e:
			print("*** " + env + " ERROR TRANSLATING TEXT")
			print(e)
	
	return output

	
def sentiment_text(sentiment_url, sentiment_apikey, sentiment_model, text):

	URL = sentiment_url + "/v1/analyze?version=2022-04-07"
	headers = {"Content-Type":"application/json"}
	data = {"text":text[:1950],
			"features":{
				"classifications":{
					"model":sentiment_model}}}
	try:
		r = requests.post(URL, auth=("apikey",sentiment_apikey), headers=headers, json=data)
		r.raise_for_status()
		for class_found in r.json()["classifications"]:
			if class_found['class_name'] == "positive":
				return class_found['confidence']
		raise
	except Exception as ex:
		print("*** " + env + " ERROR GETTING SENTIMENT:", str(ex))
		return -2

# @DEV: Uploads a document to a Watson collection
# @PARAM: _doc_path is a string of the local file you are uploading.
# @PARAM: _environment_id is the string id of the environment you wish to target.
# @PARAM: _collection_id is the string id of the collection you are uploading to.
# @PARAM: _metadata is the JSON string of objects you want to include in each document output
def add_document(_discovery_obj, _file_info, _environment_id, _collection_id, _metadata=None):
	#print("Uploading " + _metadata['title'] + ".html" + " to collection: " + _collection_id)
	add_doc = _discovery_obj.add_document(environment_id=_environment_id, collection_id=_collection_id, file=_file_info, filename=re.sub('[^A-Za-z0-9-_]+', '', _metadata['title']) + '.html',
				file_content_type="text/html", metadata=json.dumps(_metadata)).get_result()
	return json.dumps(add_doc, indent=2)


# @DEV: Uses an API to insert a row in the WM SQL DB
# @PARAM: sql_db_url is the url of the API
# @PARAM: version is the version of the API method that should be used
# @PARAM: sql_db_apikey is the apikey for the API
# @PARAM: payload is the article payload
# @RET: returns the id of the article in the SQL DB or 0 if the operation fails
def insert_sql_db(sql_db_url,version,sql_db_apikey,payload):
	try:
		params={'apikey': sql_db_apikey}
		r = requests.post(sql_db_url+version+'/add-article', params=params, json=payload)
		r.raise_for_status()
		j = r.json()
		return j['article_id']
	except Exception as e:
		print("*** " + env + " ERROR ADDING ARTICLE TO SQL DB:",str(e))
		print("PAYLOAD:",payload)
		return "0"
	


# @DEV: Uses the requests library to download the html of each url given and saves to a repository.
# @PARAM: _article_map is a Dictionary where keys map to meta data of each article.
# @PARAM: _file_dump is a String for file system path where you want to save the files.
def download_html(_article_map, _sentiment_url, _sentiment_apikey, _sentiment_model, translate_url, translate_apikey):
	for file_name in _article_map.keys():
		url = _article_map[file_name]['metadata']["url"]
		text = ""
		try:
			s = requests.Session()
			s.headers['User-Agent'] = "wrights-media-rss"
			r = s.get(url)
			r.raise_for_status()
			html = r.text
			text = re.sub('[^A-Za-z0-9-_\., ]+', '', get_article_body(html))
			if not text:
				_article_map[file_name]["metadata"]["sentiment_score"] = -3
		except Exception as ex:
			_article_map[file_name]["metadata"]["sentiment_score"] = -4
		
		text = translate_text(translate_url, translate_apikey, _article_map[file_name]["metadata"]["language"], text)
		html_doc = "<!DOCTYPE html><html><head><title>" + _article_map[file_name]['metadata']['title'] + "</title></head><body><p>" + text + "</p></body></html>"
		_article_map[file_name]["text"] = html_doc
		if _article_map[file_name]["metadata"]["lead_classifier"] > .5 and text:
			if "Dow Jones" in _article_map[file_name]["metadata"]["publisher"]:
				_article_map[file_name]["metadata"]["sentiment_score"] = -6
			else:
				_article_map[file_name]["metadata"]["sentiment_score"] = sentiment_text(_sentiment_url, _sentiment_apikey, _sentiment_model, text)
		else:
			_article_map[file_name]["metadata"]["sentiment_score"] = -5
		
	return _article_map

# @DEV: Loops through a directory of documents and calls the add_document function for each of them.
# After it is successfully uploaded to Watson Discovery, the file is removed.
# @NOTICE: At the moment we are using exponential back off up to 3 attempts for all exceptions. We should
# make the functionality specific to error code rather than general.
# @PARAM: _directory is the string for the path to the directory of files you are targetting.
# @PARAM: _article_map is a Dictionary where keys map to meta data of each article.
# @PARAM: _environment_id is a string of the IBM Cloud environment id.
# @PARAM: _collection_id is a string of the IBM Cloud collection id
# @PARAM: _sql_db_url is the SQL DB API url
# @PARAM: _sql_db_apikey is the SQL DB API apikey
def push_all_docs(_discovery_object, _article_map, _environment_id, _collection_id, _sql_db_url, _sql_db_apikey, _sql_db_enabled):
	uploaded_counter = 0
	uploaded_to_watson = {}
	for file_name in _article_map.keys():
		time_out = 5
		attempts = 1
		_article_map[file_name]['metadata']['ingestion_timestamp'] = calendar.timegm(time.gmtime())*1000
		if _sql_db_enabled:
			while True:
				try:
					payload = { "article_title": _article_map[file_name]['metadata']['title'],
								"article_publisher": _article_map[file_name]['metadata']['publisher'],
								"article_magazine": _article_map[file_name]['metadata']['feed_name'],
								"article_url": _article_map[file_name]['metadata']['url'],
								"lead_classifier": _article_map[file_name]['metadata']['lead_classifier'],
								"article_pubdate": _article_map[file_name]['metadata']['pub_date'],
								"article_text": _article_map[file_name]['text'],
								"sentiment_score": _article_map[file_name]['metadata']['sentiment_score']
								}
					sqldb_id_v2 = insert_sql_db(_sql_db_url,"v2",_sql_db_apikey,payload)
					_article_map[file_name]['metadata']['sqldb_id_v2'] = sqldb_id_v2
				except Exception as e:
					if attempts > 1:
						break
					else:
						print("Method failed with status code " + str(e.code) + ": " + e.message)
						print("Document: " + file_name)
						print("Retrying in " + str(time_out) + "seconds to upload...")
						time.sleep(time_out)
						time_out = time_out ** 2
						attempts += 1
						continue
				break
			
		attempts = 1
		while True:
			try:
				#upload to Watson Discovery
				print("METADATA TO UPLOAD:",_article_map[file_name]['metadata'])
				uploaded_to_watson[file_name] =  add_document(
					_discovery_obj = _discovery_object,
					_file_info = _article_map[file_name]['text'],
					_environment_id = _environment_id, 
					_collection_id = _collection_id, 
					_metadata = _article_map[file_name]['metadata'])
				uploaded_counter += 1
				#print(str(uploaded_counter) + " out of " + str(len(_article_map.keys())) + " documents uploaded.")
			except ApiException as ex:
				if attempts > 1:
					break
				else:
					print("Method failed with status code " + str(ex.code) + ": " + ex.message)
					print("Document: " + file_name)
					print("Retrying in " + str(time_out) + "seconds to upload...")
					time.sleep(time_out)
					time_out = time_out ** 2
					attempts += 1
					continue
			break
	return uploaded_to_watson

def main(_param_dictionary):
	global env
	env = _param_dictionary['env']
	
	discovery_object = DiscoveryV1(
		version=_param_dictionary['discovery_version'], 
		url=_param_dictionary['discovery_url'], 
		iam_apikey=_param_dictionary['discovery_api_key']
		)
	print("CALLED WITH PARAMS:",_param_dictionary)
	result = push_all_docs(discovery_object,
							download_html(_param_dictionary['parsed_feed'],_param_dictionary["sentiment_url"],_param_dictionary["sentiment_apikey"],_param_dictionary["sentiment_model"],_param_dictionary["translate_url"],_param_dictionary["translate_apikey"]),
							_param_dictionary['environment_id'],
							_param_dictionary['collection_id'],
							_param_dictionary['sql_db_url'],
							_param_dictionary['sql_db_apikey'],
							_param_dictionary['sql_db_enabled'])
	return result