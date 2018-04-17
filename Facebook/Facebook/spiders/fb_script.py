import MySQLdb
import datetime
import csv
from generic_functions import *

class Login(object):

    def __init__(self):
        self.con = MySQLdb.connect(db='facebook_profic',
                      user='root',
                      passwd='root',
                      charset="utf8",
                      host='localhost',
                      use_unicode=True)

        self.cur = self.con.cursor()
        #self.select_qry1 = 'select url,reference_url from facebook_profilepic_meta where image_path like "%fb_propic_images%" limit 1' 
        self.select_qry1 = 'select url,meta_data from facebook_crawl where date(modified_at)>="2018-01-30" limit 0,100000'
        self.header_params = ['post_id','page_id','post_updated_at','post_created_at',' post_message','post_reactions_count','reaction_type','profile_name','profile_id','profile_url','post_url']
        self.excel_file_name = 'Likes_data_for_titaneyeplus_%s.csv'%str(datetime.datetime.now().date())
        oupf = open(self.excel_file_name, 'wb+')
        self.todays_excel_file  = csv.writer(oupf)
        self.todays_excel_file.writerow(self.header_params)

    def main(self):
        self.cur.execute(self.select_qry1)
        
        rows = self.cur.fetchall()
        for row in rows:
            url,meta_data = row
            import json
            profile_url = url.replace('app_scoped_user_id/','')
            profile_id = url.split('/')[-1]
            data = json.loads(meta_data)
            like_count = data.get('summary','')
            post_id = data.get('post_id','').split('_')[-1]
            profile_name = data.get('profile_name','')
            page_id = data.get('post_id','').split('_')[0]
            created_time = data.get('created_time','')
            message = data.get('message','')
            updated_time = data.get('updated_time','')
            like_type = data.get('like_type','')
            posts_url = "https://www.facebook.com/titaneyeplus/posts/"+post_id
            vals = str(post_id),str(page_id),normalize(updated_time),str(created_time),normalize(message),str(like_count),normalize(like_type),normalize(profile_name),str(profile_id),normalize(profile_url),normalize(posts_url)
            self.todays_excel_file.writerow(vals)
   
            

        """self.cur.execute(self.select_qry1)
        rows = self.cur.fetchall()
        for row in rows:
            sk = row
            sk = sk[0]
            self.cur.execute(self.select_qry2%sk)
            count = self.cur.fetchall()
            for data in count :
                exp = data
                exp = exp[0]
                
                if 'years' in exp :
                    exp_ = exp.split('years')[0]
                    exp_ = int(exp_)
                    if exp_ > 2:
                        print sk , exp_
                        values = (sk,exp)
                        self.cur.execute(self.insert_qry,values)"""

if __name__ == '__main__':
    Login().main()

