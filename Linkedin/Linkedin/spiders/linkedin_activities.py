import os
import scrapy
import re
import json
import csv
import datetime
import requests
import time
from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from collections import OrderedDict
from generic_functions import * 
from dateutil import relativedelta

class Linkedinactivities(scrapy.Spider):
	name = "linkedinactivities_srp"
	allowed_domains = ["linkedin.com"]
	start_urls = ('https://www.linkedin.com/uas/login?goback=&trk=hb_signin',)

	def __init__(self, *args, **kwargs):
		super(Linkedinactivities, self).__init__(*args, **kwargs)
                self.login = kwargs.get('login', 'raja')
		self.url = kwargs.get('url', 'https://www.linkedin.com/in/nisha-shah-9a873416/')
		self.logins_dict = {'raja':['','']}
		self.filename = "LINKEDINACTIVITES_POC.csv"
	        self.csv_file = self.is_path_file_name(self.filename)
		self.fields = ['Activity Link', 'Subtitle', 'share descrition', 'title', 'share_url', 'post description', 'urn', 'article type', 'resolved url', 'article/feed image', 'Feed share by image', 'Feed share by [original name]'] 
		self.csv_file.writerow(self.fields)
                dispatcher.connect(self.spider_closed, signals.spider_closed)
                self.domain = "https://www.linkedin.com"

	def parse(self, response):
                sel = Selector(response)
                logincsrf = ''.join(sel.xpath('//input[@name="loginCsrfParam"]/@value').extract())
		csrf_token = ''.join(sel.xpath('//input[@id="csrfToken-login"]/@value').extract())
                source_alias = ''.join(sel.xpath('//input[@name="sourceAlias"]/@value').extract())
		login_account = self.logins_dict[self.login]
		account_mail, account_password = login_account
		return [FormRequest.from_response(response, formname = 'login_form',\
			formdata={'session_key':account_mail,'session_password':account_password,'isJsEnabled':'','source_app':'','tryCount':'','clickedSuggestion':'','signin':'Sign In','session_redirect':'','trk':'hb_signin','loginCsrfParam':logincsrf,'fromEmail':'','csrfToken':csrf_token,'sourceAlias':source_alias},callback=self.parse_next, meta={'csrf_token':csrf_token})]

	def is_path_file_name(self, excel_file_name):
		if os.path.isfile(excel_file_name):
    			os.system('rm %s' % excel_file_name)
		oupf = open(excel_file_name, 'ab+')
		todays_excel_file = csv.writer(oupf)
		return todays_excel_file

    	def spider_closed(self, spider):
		cv = requests.get('https://www.linkedin.com/logout/').text

	def parse_next(self, response):
		sel = Selector(response)
		csrf_token = response.meta.get('csrf_token', '')
		if self.url:
			yield Request(self.url, callback=self.parse_profilepage, meta = {'csrf_token':csrf_token})


	def parse_profilepage(self, response):
                sel = Selector(response)
                cooki_list = response.request.headers.get('Cookie', [])
                li_at_cookie = ''.join(re.findall('li_at=(.*?); ', cooki_list))
                headers = {
                    'cookie': 'li_at=%s;JSESSIONID="%s"' % (li_at_cookie, response.meta['csrf_token']),
                    'x-requested-with': 'XMLHttpRequest',
                    'csrf-token': response.meta['csrf_token'],
                    'authority': 'www.linkedin.com',
                    'referer': 'https://www.linkedin.com/',
                }
		profil_data = ''
		try:
			profile_data = json.loads(sel.xpath('//code[contains(text(),"profile.ProfileView")]/text()').extract()[0])['data']['profile']
			if profile_data:
				profil_data = profile_data.replace('urn:li:fs_profile:', '')
		except:
			pass
		if profil_data:
			api_compid_url = "https://www.linkedin.com/voyager/api/feed/updates?count=5&moduleKey=member-activity:phone&numComments=0&numLikes=0&profileId=%s&q=memberFeed" % profil_data
			yield Request(api_compid_url, callback = self.parse_correct, meta = {
		    'csrf_token': response.meta['csrf_token'], 'headers':headers, 'api_url':api_compid_url
		}, headers = headers)

	def parse_correct(self, response):
                data = json.loads(response.body)
		api_compid_url = response.meta.get('api_url', '')
		headers = response.meta.get('headers', {})
		data_elements = data.get('elements', [])
		for datae in data_elements:
			permalink = datae.get('permalink', '')
			keys_va = datae.get('value', {}).keys()[0]
			whole_tx = datae.get('value', {}).get(keys_va)
			share_con = whole_tx.get('originalUpdate', {}).get('value', {}).get('com.linkedin.voyager.feed.ShareUpdate', {}).get('content', {})
			if not share_con:
				share_con = whole_tx.get('originalUpdate', {}).get('value', {}).get('com.linkedin.voyager.feed.Reshare', {}).get('originalUpdate', {}).get('value', {}).get('com.linkedin.voyager.feed.ShareUpdate', {}).get('content', {})
			origna_up = whole_tx.get('originalUpdate', {}).get('value', {}).get('com.linkedin.voyager.feed.ShareUpdate', {}).get('actor', {})
			if not origna_up:
				origna_up = whole_tx.get('originalUpdate', {}).get('value', {}).get('com.linkedin.voyager.feed.Reshare', {}).get('actor', {})
			orinna_kys = origna_up.keys()
			if orinna_kys:
				mini_prifil = origna_up.get(orinna_kys[0], {}).get('miniProfile', {})
				if not mini_prifil:
					mini_prifil = origna_up.get(orinna_kys[0], {}).get('miniCompany', {})
			share_post_orinal_name = mini_prifil.get('name', '')
			if not share_post_orinal_name:
				share_post_orinal_finame = mini_prifil.get('firstName', '')
				share_post_orinal_laname = mini_prifil.get('lastName', '')
				share_post_orinal_name = '%s%s%s' % (share_post_orinal_finame, ' ', share_post_orinal_laname)
			pict_origina = mini_prifil.get('picture', {})
			if not pict_origina:
				pict_origina = mini_prifil.get('logo', {})
			root_url_pictvec = pict_origina.get('com.linkedin.common.VectorImage', {})
			root_url_pict = root_url_pictvec.get('rootUrl', '')
			widhth_finla = root_url_pictvec.get('artifacts', [])
			if widhth_finla:
				widhth_finla = widhth_finla[-1].get('fileIdentifyingUrlPathSegment', '')
			final_root_widht =''
			if widhth_finla and root_url_pict:
				final_root_widht = "%s%s" % (root_url_pict, widhth_finla)
			share_keys = share_con.keys()
			if share_keys:
				share_key_one = share_keys[0]
			share_update = share_con.get(share_key_one , {})
			subtitle = share_update.get('subtitle', '')
			share_description = share_update.get('description', '')
			if not share_description:
				share_description = whole_tx.get('attributedText', {}).get('text', '')
			title = share_update.get('title', '')
			share_url = share_update.get('url', '')
			descii_list = share_update.get('text', {}).get('values', [])
			desc_fi = []
			for de in descii_list:
				desc_ = de.get('value', '')
				desc_fi.append(desc_)
			post_desc_fi = ' '.join(desc_fi)
			urn = share_update.get('urn', '')
			article_typ = share_update.get('articleType', '')
			resolved_url = share_update.get('resolvedUrl', '')
			article_image = share_update.get('image', {}).get('com.linkedin.voyager.common.MediaProxyImage', {}).get('url', '')
 			values = [permalink, subtitle, share_description, title, share_url, post_desc_fi, urn, article_typ, resolved_url, article_image, final_root_widht, share_post_orinal_name ]
			values = [normalize(i) for i in values]
			self.csv_file.writerow(values)
	
		url_paging  = data.get('paging',[])
		if url_paging:
			count_data = url_paging.get('count','')
			start_data = url_paging.get('start','')
			total_data = url_paging.get('total','')
			pagination_token = data.get('metadata', {}).get('paginationToken', '')
			if total_data > count_data+start_data and pagination_token:
				retrun_url = "%s%s%s%s%s%s"%(api_compid_url, '&', 'paginationToken=', pagination_token,'&start=', start_data+count_data)
				yield Request(retrun_url, headers=headers, callback=self.parse_correct, meta={'api_url':api_compid_url, 'headers':headers})
