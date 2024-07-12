# Standard library imports
import json
import requests
import calendar
import time
import re
import os
# Third party imports
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
		cur_length = 0
		for seg in bp:
			cur_length += len(seg)
		if cur_length > max_length:
			max_length = cur_length
			body = bp
	if max_length < 1:
		body = dom.xpath('//p/text()')
		
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
		"es-ES": "ES",
		"NL": "NL",
		"nl": "NL",
		"fr": "FR",
		"de": "DE",
		"it": "IT",
		"es": "ES"
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
		raise
	


# @DEV: Uses the requests library to download the html of each url given and saves to a repository.
# @PARAM: _article_map is a Dictionary where keys map to meta data of each article.
# @PARAM: _file_dump is a String for file system path where you want to save the files.
def download_html(_article_map, _sentiment_url, _sentiment_apikey, _sentiment_model, translate_url, translate_apikey):
	for file_name in _article_map.keys():
		url = _article_map[file_name]['metadata']["url"]
		text = ""
		if 'article_text' in _article_map[file_name]['metadata'] and _article_map[file_name]['metadata']['article_text'] != "":
			text = re.sub('[^A-Za-z0-9-_\., ]+', '', get_article_body(_article_map[file_name]['metadata']['article_text']))
		else:
			html = ""
			try:
				s = requests.Session()
				s.headers['User-Agent'] = "wrights-media-rss"
				r = s.get(url)
				html = r.text
				r.raise_for_status()
				text = re.sub('[^A-Za-z0-9-_\., ]+', '', get_article_body(html))
				if not text:
					_article_map[file_name]["metadata"]["sentiment_score"] = -3
					print("*** " + env + " EMPTY ARTICLE TEXT. TITLE: " + _article_map[file_name]['metadata']['title'] + " ERROR TEXT: ",html)
			except Exception as ex:
				_article_map[file_name]["metadata"]["sentiment_score"] = -4
				print("*** " + env + " ERROR READING ARTICLE TEXT. TITLE: " + _article_map[file_name]['metadata']['title'] + " ERROR TEXT: ",str(ex),html)
		
		_article_map[file_name]["text"] = text
		if text:
			if _article_map[file_name]["metadata"]["lead_classifier"] > .5:
				text = translate_text(translate_url, translate_apikey, _article_map[file_name]["metadata"]["language"], text)
				_article_map[file_name]["text"] = text
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
def push_all_docs(_article_map, _environment_id, _collection_id, _sql_db_url, _sql_db_apikey, _sql_db_enabled, lead_by_article_url):
	uploaded_counter = 0
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
					uploaded_counter += 1
				except Exception as e:
					if attempts > 2:
						print("*** " + env + " ARTICLE NOT ADDED TO SQL DB:",file_name)
						break
					else:
						time.sleep(time_out)
						time_out = time_out ** 2
						attempts += 1
						continue
				break
		
		if (_article_map[file_name]['metadata']['sentiment_score'] > .33 and _article_map[file_name]['metadata']['lead_classifier'] > .5) or (_article_map[file_name]['metadata']['lead_classifier'] > .45 and "Dow Jones" in _article_map[file_name]['metadata']['article_publisher']):
			time_out = 5
			attempts = 1
			while True:
				try:
					r = requests.get(url=lead_by_article_url+'?article_id=' + str(_article_map[file_name]['metadata']['sqldb_id_v2']))
					r.raise_for_status()
				except Exception as ex:
					if attempts > 2:
						print("*** " + env + " ERROR CALLING LEAD-BY-ARTICLE", str(ex))
						break
					else:
						time.sleep(time_out)
						time_out = time_out ** 2
						attempts += 1
						continue
				break
				
	return uploaded_counter

def main(_param_dictionary):
	global env
	inputs = os.environ
	env = inputs['env']
	
	#print("CALLED WITH PARAMS:",_param_dictionary)
	result = push_all_docs(download_html(_param_dictionary['parsed_feed'],inputs["sentiment_url"],inputs["sentiment_apikey"],inputs["sentiment_model"],inputs["translate_url"],inputs["translate_apikey"]),
							inputs['environment_id'],
							inputs['collection_id'],
							inputs['sql_db_url'],
							inputs['sql_db_apikey'],
							inputs['sql_db_enabled'],
							inputs['lead_by_article_url'])


	
	return {
		"headers": {
			"Content-Type": "application/json",
		},
		"statusCode": 200,
		"body": {"uploaded_docs_count": result}
	}