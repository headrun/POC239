from fb_constants import *
from fb_browse_queries import *
import sys
from generic_functions import *

class FacebookPostsbrowse(BaseSpider):
    name = "facebook_posts_crawler"
    start_urls = ['https://www.facebook.com/login']
    handle_httpstatus_list = [404, 302, 303, 403, 500]

    def __init__(self, *args, **kwargs):
        super(FacebookPostsbrowse, self).__init__(*args, **kwargs)
        self.login = kwargs.get('login','yagnasree@headrun.com')
	self.modified_at_crawl  = kwargs.get('mpi', '')
	self.domain = "https://mbasic.facebook.com"
	self.con, self.cur = get_mysql_connection(DB_HOST, REQ_DB_NAME, '')
	self.cur.execute(get_qry_params)
	self.profiles_list = [i for i in self.cur.fetchall()]
	self.res_afterlogin = ''
	self.cur_date = str(datetime.datetime.now().date())
        self.myname = os.path.basename(__file__).replace(".py", '')
        self.log = init_logger("%s_%s.log" %(self.myname,self.cur_date))
	dispatcher.connect(self.spider_closed, signals.spider_closed)
        
    def spider_closed(self, spider):
	close_mysql_connection(self.con, self.cur)
	if self.res_afterlogin:
		login_url = self.res_afterlogin.xpath('//a[contains(@href,"/logout.php")]/@href').extract()
		if login_url:
			login_urlf =  "%s%s"%(self.domain,login_url[0])
			cv = requests.get(login_urlf).text
			data = Selector(text=cv)
			login_xpat = data.xpath('//a[contains(@href,"/login.php")]/@href')
			if  login_xpat: self.log.info("Message - %s" %("Logout Successfully"))

    def parse(self, response):
        sel = Selector(response)
        if self.profiles_list  :
                login  = constants_dict[self.login]
                lsd = ''.join(sel.xpath('//input[@name="lsd"]/@value').extract())
                lgnrnd = ''.join(sel.xpath('//input[@name="lgnrnd"]/@value').extract())
                return [FormRequest.from_response(response, formname = 'login_form',\
                                formdata={'email': login[0],'pass':login[1],'lsd':lsd, 'lgnrnd':lgnrnd},callback=self.parse_redirect)]


    def parse_close(self, response):
	sel = Selector(response)
	self.res_afterlogin = sel


    def parse_redirect(self,response):
        sel = Selector(response)
        if 'Your account has been disabled' in response.body :
            noti_xpath = 'Your account has been disabled'
            user = constants_dict[self.login][0]
            pwd = constants_dict[self.login][1]
            #self.send_mail(noti_xpath,user,pwd)
        yield Request('https://mbasic.facebook.com/support/?notif_t=feature_limits',callback=self.parse_next)

    def parse_next(self, response):
	yield Request(self.domain, callback=self.parse_close)
        sel = Selector(response)
        noti_xpath = "".join(sel.xpath('//div//span[contains(text(),"temporarily")]//text()').extract())
        if noti_xpath :
                    user = constants_dict[self.login][0]
                    pwd = constants_dict[self.login][1]
                    self.profiles_list = []
                    #self.send_mail(noti_xpath,user,pwd)
        for profilei in self.profiles_list:
            sk = profilei[0]
            try : meta_data = json.loads(profilei[2])
            except : 
                meta_data = json.dumps(str(profilei[2]))
                meta_data = json.loads(str(meta_data))
	    profile = meta_data.get('mbasic_url','')
	    email_address = meta_data.get('email_address','')
	    if not profile:
		continue
	    vals = (sk, profilei[1], sk, profilei[1])
	    #self.cur.execute(qry_params, vals)
	    #self.cur.execute(update_get_params%(9,sk))
	    #self.update_status(sk, 9, 'facebook_crawl', update_get_params)
            yield Request(profile, callback=self.parse_profile, meta={'sk':sk,"al":'',"see_more":'','profile':profile,'email_address':email_address},dont_filter=True)


    def update_status(self, sk, crawl_status, table_name, update_qrys):
        delete_query = 'DELETE FROM %s WHERE crawl_status=%s AND sk ="%s" and modified_at < "%s"' % (table_name, crawl_status, sk, self.modified_at_crawl)
        execute_query(self.cur, delete_query)
        bkup_query = 'select sk from %s where sk = "%s" group by sk  having count(sk)>1' % (table_name, sk)
        try: self.cur.execute(update_qrys % (crawl_status, sk))
        except: 
                try:
                        recs_ = fetchall(self.cur, bkup_query)
                        if recs_:
                                query2 = 'select max(modified_at) from %s where sk ="%s"'%(table_name, sk)
                                recs_1 = fetchall(self.cur, query2)
                                del_qu = "delete from %s where sk ='%s' and modified_at not like '%s'" % (table_name, sk, str(recs_1[0][0]))
                                execute_query(self.cur, del_qu)
                                self.cur.execute(update_qrys % (crawl_status, sk))
                except: 
                        pass

    def parse_profile(self, response):
        sel = Selector(response)
        sk = response.meta['sk']
        time_line_url = self.domain + "".join(sel.xpath('//div//a[contains(@href,"=timeline")]//@href').extract())
        if time_line_url : 
            yield Request(time_line_url, callback=self.parse_timeline, meta={'sk':sk},dont_filter=True)

    def parse_timeline(self,response):
	sel = Selector(response)
        sk = response.meta['sk']
        feed_links = sel.xpath('//div[@id="tlFeed"]//a[contains(@href,"yearSections")]//@href').extract()
        for feed in feed_links :
            if feed : 
                feed = self.domain + feed
                yield Request(feed, callback=self.parse_feed, meta={'sk':sk},dont_filter=True)
        yield Request("https://mbasic.facebook.com/prashant.iyer.3133?yearSectionsYears=1969%2C2013%2C2012%2C2011%2C2010%2C2009%2C2008&sectionLoadingID=m_timeline_loading_div_1325404799_1293868800_8_&lst=100001201522302%3A736695933%3A1523871389&timeend=1325404799&timestart=1293868800&tm=AQA-2XEjQaXhvH0v&refid=17", callback=self.parse_feed, meta={'sk':sk},dont_filter=True)

    def parse_feed(self,response):
	sel = Selector(response)
        data = {}
        sk = response.meta['sk']
        video_link,video_desc,video_image = '','',''
        import pdb;pdb.set_trace()
        nodes = sel.xpath('//div[@id="structured_composer_async_container"]//div[@class="bl bz ca"]')
        for node in nodes : 
            post_id =  json.loads("".join(node.xpath('./@data-ft').extract())).get('top_level_post_id','')
	    posted_by_link =  "".join(node.xpath('''.//h3[@data-ft='{"tn":"C"}']//span//strong//a//@href''').extract()) or "".join(node.xpath('''.//h3[@data-ft='{"tn":"C"}']//strong//a//@href''').extract())
            if posted_by_link : posted_by_link = self.domain + posted_by_link
	    posted_by =  "".join(node.xpath('''.//h3[@data-ft='{"tn":"C"}']//span//strong//a//text()''').extract()) or "".join(node.xpath('''.//h3[@data-ft='{"tn":"C"}']//strong//a//text()''').extract())
	    datetime =  "".join(node.xpath('''.//div[@data-ft='{"tn":"*W"}']//abbr//text()''').extract())
	    post_desc = "".join(node.xpath('''.//div[@data-ft='{"tn":"*s"}']//span//div//text()''').extract()) or "".join(node.xpath('''.//div[@data-ft='{"tn":"*s"}']//text()''').extract())  or  "".join(node.xpath('''.//div[@data-ft='{"tn":"*s"}']//span//text()''').extract()) 
            video_link = "".join(node.xpath('//a[contains(@href,"/video_redirect/")]//@href').extract()) or "".join(node.xpath('''.//div[@data-ft='{"tn":"H"}']//a[contains(@href,"/lm.facebook.com")]//@href''').extract())
            if video_link and 'http'  not in video_link:
                video_link = self.domain + video_link
                video_desc = "".join(node.xpath('''.//div[@data-ft='{"tn":"*s"}']//span//div//text()''').extract())
                video_image = "".join(node.xpath('''.//div[@data-ft='{"tn":"H"}']//a//img/@src''').extract())
            if 'facebook.com' in video_link :
                video_desc =  "".join(node.xpath('''.//div[@data-ft='{"tn":"H"}']//h3//text()''').extract())
 
	    post_privacy_status = "".join(node.xpath('''.//div[@data-ft='{"tn":"*W"}']//span//span//text()''').extract())
            comments = "".join(node.xpath('''.//div[@data-ft='{"tn":"*W"}']//a[contains(text(),"Comment")]//text()''').extract())
            photo_link = "".join(node.xpath('''.//div[@data-ft='{"tn":"E"}']//a[contains(@href,"/photo.php?")]//@href''').extract())
            if photo_link : 
                photo_lnk = self.domain + photo_link
                image_link = "".join(node.xpath('''.//div[@data-ft='{"tn":"E"}']//a//img//@src''').extract())
                yield Request(photo_lnk, callback=self.parse_fullstory, meta={'sk':sk,'post_id':post_id,'posted_by_link':posted_by_link,'posted_by':posted_by,'datetime':datetime,'post_desc':post_desc,'post_privacy_status':post_privacy_status,'comments':comments,'feed_link':response.url,'image_link':image_link,'video_link':video_link,'video_desc':video_desc,'video_image':video_image},dont_filter=True)
            
            else : 
                full_link = "".join(node.xpath('.//a[contains(text(),"Full Story")]//@href').extract())
                if full_link :
                    full_link = self.domain + str(full_link)
                    yield Request(full_link, callback=self.parse_fullstory,meta={'sk':sk,'post_id':post_id,'posted_by_link':posted_by_link,'posted_by':posted_by,'datetime':datetime,'post_desc':post_desc,'post_privacy_status':post_privacy_status,'comments':comments,'feed_link':response.url,'video_link':video_link,'video_desc':video_desc,'video_image':video_image},dont_filter=True)

        see_more  = sel.xpath('//a[contains(text(),"Show more")]//@href').extract()
        if see_more : 
            see_more = self.domain + "".join(see_more)
            yield Request(see_more,callback=self.parse_feed, meta={'sk':sk},dont_filter=True)


    def parse_fullstory(self, response):
	sel = Selector(response)
        #video_link,video_desc,video_image = '','',''
	sk = response.meta['sk']
        post_id = response.meta.get('post_id','')
        posted_by_link = response.meta.get('feed_link','')
        posted_by = response.meta.get('posted_by','')
        datetime = response.meta.get('datetime','')
        post_desc = response.meta.get('post_desc','')
        post_privacy_status = response.meta.get('post_privacy_status','')
        comments = response.meta.get('comments','')
        post_image_link = response.meta.get('image_link','')
        """video_link = "".join(sel.xpath('//a[contains(@href,"/video_redirect/")]//@href').extract()) or "".join(sel.xpath('''.//div[@data-ft='{"tn":"H"}']//a[contains(@href,"/lm.facebook.com")]//@href''').extract())
        if video_link and 'http'  not in video_link:
                video_link = self.domain + video_link
                video_desc = "".join(sel.xpath('''.//div[@data-ft='{"tn":"*s"}']//span//div//text()''').extract())
                video_image = "".join(sel.xpath('''.//div[@data-ft='{"tn":"H"}']//a//img/@src''').extract())
        if 'facebook.com' in video_link :
                video_desc =  "".join(sel.xpath('''.//div[@data-ft='{"tn":"H"}']//h3//text()''').extract())"""
        video_link = response.meta.get('video_link','')
        video_desc = response.meta.get('video_desc','')
        video_image =  response.meta.get('video_image','')
        likes_link = "".join(sel.xpath('//a[contains(@href,"/ufi/reaction/profile/browser/")]//@href').extract())
        if likes_link : 
            likes_link = self.domain + likes_link
        likes_count = "".join(sel.xpath('//a[contains(@href,"/ufi/reaction/profile/browser/")]//following-sibling::div//text()').extract())
        if likes_link : 
            yield Request(likes_link, callback=self.parse_emoji, meta={'sk':sk,'post_id':post_id,'posted_by_link':posted_by_link,'posted_by':posted_by,'datetime':datetime,'post_desc':post_desc,'post_privacy_status':post_privacy_status,'comments':comments,'post_image_link':post_image_link,'reference_url':response.url,'video_link':video_link,'video_desc':video_desc,'video_image':video_image},dont_filter=True)
        else :
             vals = (post_id,sk,0,response.url,video_link,video_image,post_desc,datetime,post_image_link,posted_by,posted_by_link,comments,0,0,0,0,0,0,0,video_desc)
             vals = vals + vals
             self.cur.execute(qry_params,vals)

    def parse_emoji(self,response):
        sel = Selector(response)     
        sk = response.meta['sk']
        post_id = response.meta.get('post_id','')
        posted_by_link = response.meta.get('posted_by_link','')
        posted_by = response.meta.get('posted_by','')
        datetime = response.meta.get('datetime','')
        post_desc = response.meta.get('post_desc','')
        post_privacy_status = response.meta.get('post_privacy_status','')
        comments = response.meta.get('comments','')
        ref_url = response.meta.get('reference_url','')
        video_desc = response.meta.get('video_desc','')
        video_link = response.meta.get('video_link','')
        video_image = response.meta.get('video_image','')
        post_image_link = response.meta.get('post_image_link','')
        like = str("".join(sel.xpath('//img[contains(@alt,"Like")]//following-sibling::span//text()').extract()))
        wow = str("".join(sel.xpath('//img[contains(@alt,"Wow")]//following-sibling::span//text()').extract()))
        sad = str("".join(sel.xpath('//img[contains(@alt,"Sad")]//following-sibling::span//text()').extract()))
        love = str("".join(sel.xpath('//img[contains(@alt,"Love")]//following-sibling::span//text()').extract()))
        haha = str("".join(sel.xpath('//img[contains(@alt,"Haha")]//following-sibling::span//text()').extract()))
        angry =  str("".join(sel.xpath('//img[contains(@alt,"Angry")]//following-sibling::span//text()').extract()))
	lll = [like, wow, sad, love, haha, angry]
        total = sum([int(i) for i in lll if i])
        vals = (post_id,sk,0,ref_url,video_link,video_image,post_desc,datetime,post_image_link,posted_by,posted_by_link,comments,total,like,love,wow,haha,sad,angry,video_desc)
        vals = vals + vals
        self.cur.execute(qry_params,vals)
        
         
