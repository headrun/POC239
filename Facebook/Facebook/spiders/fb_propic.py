from fb_constants import *
from fb_browse_queries import *
import sys
from generic_functions import *
import urllib
import os
import re

class Facebookpropicbrowse(BaseSpider):
    name = "facebookpropic_browse"
    start_urls = ['https://www.facebook.com/login']
    handle_httpstatus_list = [404, 302, 303, 403, 500]

    def __init__(self, *args, **kwargs):
        super(Facebookpropicbrowse, self).__init__(*args, **kwargs)
        self.login = kwargs.get('login','yagnasree@headrun.com')
	self.modified_at_crawl  = kwargs.get('mpi', '')
	self.domain = "https://mbasic.facebook.com"
	self.con = MySQLdb.connect(db='facebook_profic',
                                   user='root',
                                   passwd='root',
                                   charset="utf8",
                                   host='localhost',
                                   use_unicode=True)
        self.cur = self.con.cursor() 
	self.cur.execute(get_qry_params)
	self.profiles_list = [i for i in self.cur.fetchall()]
	self.res_afterlogin = ''
	self.cur_date = str(datetime.datetime.now().date())
        self.myname = os.path.basename(__file__).replace(".py", '')
        self.log = init_logger("%s_%s.log" %(self.myname,self.cur_date))
	dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.insert_qry = "insert into facebook_profilepic_meta(sk,url,data_size,image_width,image_height,image_path,image_url,aux_info,reference_url,created_at,modified_at)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()) on duplicate key update modified_at = now()"
        self.update_qry = 'update facebook_crawl set crawl_status = 1 where sk = "%s"'
        self.update_qry_ = 'update facebook_crawl2 set crawl_status = 1 where sk = "%s"'
        self.update_qry2 = 'update facebook_crawl set crawl_status = 2 where sk = "%s"'
        self.update_qry2_ = 'update facebook_crawl2 set crawl_status = 2 where sk = "%s"'
        
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
            self.send_mail(noti_xpath,user,pwd)
        yield Request('https://mbasic.facebook.com/support/?notif_t=feature_limits',callback=self.parse_next)

    def parse_next(self, response):
	yield Request(self.domain, callback=self.parse_close)
        sel = Selector(response)
        noti_xpath = "".join(sel.xpath('//div//span[contains(text(),"temporarily")]//text()').extract())
        if noti_xpath :
                    user = constants_dict[self.login][0]
                    pwd = constants_dict[self.login][1]
                    self.profiles_list = []
                    self.send_mail(noti_xpath,user,pwd)
        for profilei in self.profiles_list:
            sk = profilei[0]
            profile = profilei[1]
            if profile : 
                user_id = profile.split('/')[-1]
                prof_name = profilei[2]
                if not prof_name : continue
                user_id = '1892513187726079'
                profile = "https://graph.facebook.com/"+str(user_id)+"/picture?type=large&width=720&height=720"
	    if not profile:
		continue
            headers = {
            'pragma': 'no-cache',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.8,en;q=0.6',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'cache-control': 'no-cache',
            'authority': 'graph.facebook.com',
}
            yield Request(profile,callback=self.parse_link,headers=headers,meta={'sk':sk,'prof_name':prof_name})

    def parse_link(self,response):
        sk = response.meta['sk']
        import pdb;pdb.set_trace()
        id_ = ''
        image_link = response.headers.get('Location','')
        try : id_ = response.headers.get('Location','').split('/')[-1].split('_')[1]
        except : print "no_id"
        if id_ : 
                profile_link = "https://www.facebook.com/photo.php?fbid="+str(id_) 
                yield Request(profile_link,callback=self.parse_propic,meta={'sk':sk,'image_link':image_link,'prof_name':response.meta['prof_name']})
        else : 
                values = (sk,response.url,'','','','','','image_not_available',response.url)
                self.cur.execute(self.insert_qry,values)
                try : self.cur.execute(self.update_qry%sk)
                except : print "update error"
                self.con.commit()
                self.cur.execute(self.update_qry_%sk)
                self.con.commit()


    def parse_propic(self,response):
    	sel = Selector(response)
        img_link = response.meta['image_link']
        prof_name = response.meta['prof_name']
        sk = response.meta['sk']
        prof_url =  re.findall('\"*profileLink(.*?)hc_ref', str(response.body))
        
        if prof_url: 
            prof_url = normalize("".join(prof_url)).strip('&').replace('"','').strip().replace("href=",'')
            if 'php?' in prof_url : prof_name = prof_name.replace(" ",'.')
            else : prof_name = prof_url.split('/')[-1].strip('?')

      
        else : 
            values = (sk,response.url,'','','','','',"Page_not_available",response.url)
            self.cur.execute(self.insert_qry,values)
            try : self.cur.execute(self.update_qry%sk)
            except : print "update error"
            self.con.commit()


        try : 
            reference_url = re.findall('\"*permalink(.*?)&type=3', str(response.body))[-1].replace(':','').replace('"','')
            image_id = str(prof_name)+'_'+reference_url.split('.')[-1]
	    sk = response.meta['sk']
	    image = urllib.URLopener()
            real_path =  os.path.dirname(os.path.realpath(__file__))
	    os.chdir("%s%s" % (real_path, '/new_pics'))
	    try : image_name = image.retrieve("".join(img_link), '%s.jpg'%str(image_id))
	    except : print sk
	    try : img_path = os.path.dirname(os.path.abspath(image_name[0]))+ '/' +image_name[0]
	    except :
                open('fb_no.txt','ab+').write('%s\n'%url)
	    img_width=img_height=720
	    img_data = urllib.urlopen(img_link)
	    size = img_data.headers.get("content-length",'')
	    if size : data_size = str(round(float(size)/1024,2)) + 'Kb'
	    os.chdir(real_path)
	    values = (sk,prof_url,data_size,img_width,img_height,str(img_path),img_link,'available',reference_url)
	    self.cur.execute(self.insert_qry,values)
	    try : self.cur.execute(self.update_qry2%sk)
	    except : print "error in update"
	    self.cur.execute(self.update_qry2_%sk)
	    self.con.commit()
        except : 
                values = (sk,response.url,'','','','','',"Page_not_available",response.url)
                self.cur.execute(self.insert_qry,values)
                try : self.cur.execute(self.update_qry%sk)
                except : print "update error"
                self.con.commit()
                self.cur.execute(self.update_qry_%sk)
                self.con.commit()
        
