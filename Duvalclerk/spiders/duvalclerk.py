import csv
import datetime
import json

import scrapy
from nameparser import HumanName
from scrapy import Request, Selector


class DuvalclerkSpider(scrapy.Spider):
    name = 'duvalclerk'
    custom_settings = {
        'FEED_URI': f'Output/duvalclerk_{datetime.datetime.now().strftime("%Y-%m-%d, %H:%M")}.csv',
        'FEED_FORMAT': 'csv',
    }
    listing_url = 'https://core.duvalclerk.com/internal/CoreWebSvc.asmx/CaseSearchBegin'
    login_url = 'https://core.duvalclerk.com/internal/CoreWebSvc.asmx/InteractiveLogin'
    login_payload = '{"username":"","password":""}'  # TODO PLEASE ENTER ACCOUNT DETAILS HERE
    listing_payload = {"token": "", "returnTabId": 1,
                       "inputs": {"__type": "DuvalClerk.Web.Core.UserSearchInput", "1": "a", "2": "N", "3": "3",
                                  "4": "1", "5": "0",
                                  "6": "b", "7": "0", "8": "e", "9": "2", "10": "5", "11": "_", "12": "1", "13": "3",
                                  "14": "e",
                                  "15": "6", "16": "_", "17": "4", "18": "8", "19": "5", "20": "c", "21": "_",
                                  "22": "b", "23": "4",
                                  "24": "f", "25": "e", "26": "_", "27": "3", "28": "b", "29": "8", "30": "c",
                                  "31": "7", "32": "6",
                                  "33": "6", "34": "d", "35": "0", "36": "e", "37": "e", "38": "2", "0": "N",
                                  "CaseTypeList": "485",
                                  "UseNamePhoneticMatchOption": True, "UseNameBeginsWithOption": False,
                                  "CaseStatusList": "1,3,9,2,6,12",
                                  "CaseAge": "100", "CaseMemberFilter": ""}, "captcha": "captchaPH"}
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json; charset=UTF-8',
        'Origin': 'https://core.duvalclerk.com',
        'Pragma': 'no-cache',
        'Referer': 'https://core.duvalclerk.com/CoreCms.aspx',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/108.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"'
    }

    next_page_url = "https://core.duvalclerk.com/internal/CoreWebSvc.asmx/CaseSearchUpdate"
    next_page_payload = {"token": "", "returnTabId": 1, "operation": "NextPage",
                         "requestState": "", "captcha": "captchaPH"}

    case_url = "https://core.duvalclerk.com/internal/CoreWebSvc.asmx/GetCaseById"
    case_payload = {'token': '', 'returnTabId': 1, 'caseID': '', 'simCtrl': 0}

    def start_requests(self):
        yield Request(
            url=self.login_url,
            headers=self.headers,
            body=self.login_payload,
            method='POST'
        )

    def parse(self, response, **kwargs):
        page_url = "https://core.duvalclerk.com/internal/CoreWebSvc.asmx/GetNewSearchTab"
        page_payload = {'token': '', 'returnTabId': 1}
        json_data = json.loads(response.text)
        token = json_data.get('d', {}).get('Token', '')
        page_payload['token'] = token

        yield Request(
            url=page_url,
            headers=self.headers,
            body=json.dumps(page_payload),
            callback=self.parse_values,
            method='POST',
            meta={'token': token, 'page_token': 'Random'}
        )

    def parse_values(self, response):
        for row in csv.DictReader(open('input.csv')):
            case_type = row['document type']
            case_age = row['case age']
            json_data = json.loads(response.text)
            json_html = json_data.get('d', {}).get('BodyHtml', '')
            html = Selector(text=json_html)
            case_type_id = html.xpath(f'//option[contains(text(),"{case_type}")]//@value').get()
            self.listing_payload['inputs']['CaseTypeList'] = case_type_id
            self.listing_payload['inputs']['CaseAge'] = case_age
            self.listing_payload['token'] = response.meta['token']
            yield Request(
                url=self.listing_url,
                headers=self.headers,
                body=json.dumps(self.listing_payload),
                callback=self.parse_listing,
                method='POST',
                meta={'token': response.meta['token'],
                      'page_token': [''],
                      'case_type': case_type}
            )

    def parse_listing(self, response):
        json_data = json.loads(response.text)
        json_html = json_data.get('d', {}).get('BodyHtml', '')
        html = Selector(text=json_html)
        next_page_token = ''.join(html.css('input[type="hidden"]::attr(value)').getall())
        case_ids = html.css('.searchResultsTable tbody::attr(onclick)').getall()
        for case in case_ids:
            case_num = case.replace('getCaseTabByCaseId(', '').replace(');', '')
            self.case_payload['token'] = response.meta['token']
            self.case_payload['caseID'] = case_num
            yield Request(
                url=self.case_url,
                headers=self.headers,
                body=json.dumps(self.case_payload),
                callback=self.case_detail_page,
                method='POST',
                meta={'case_num': case_num, 'case_type': response.meta['case_type']}
            )
        self.next_page_payload['requestState'] = next_page_token
        match_case = case_ids[0].replace('getCaseTabByCaseId(', '').replace(');', '')
        if match_case not in response.meta['page_token']:
            self.next_page_payload['token'] = response.meta['token']
            yield Request(
                url=self.next_page_url,
                headers=self.headers,
                body=json.dumps(self.next_page_payload),
                callback=self.parse_listing,
                method='POST',
                meta={'token': response.meta['token'],
                      'page_token': case_ids,
                      'case_type': response.meta['case_type']}
            )

    def case_detail_page(self, response):
        json_data = json.loads(response.text)
        json_html = json_data.get('d', {}).get('BodyHtml', '')
        html = Selector(text=json_html)
        case_num = html.css('#c_CaseNumberLabel::text').get()
        case_status = html.xpath('//*[contains(text(),"Case Status")]/following::td[1]/text()').get()
        file_date = html.xpath('//*[contains(text(),"File Date")]/following::td[1]//text()').get()

        d_name = '//td[contains(text(),"DECEDENT")]/ancestor::tr[1]/td[1]//*[contains(@style,"text-decoration")]//text()'
        d_address = '//td[contains(text(),"DECEDENT")]/ancestor::tr[1]/td[3]/address[descendant-or-self::text()]'
        defendant_name = html.xpath(d_name).get('')
        defendant_address = html.xpath(d_address).get('').replace('<address>', '').replace('</address>', '')

        b_name = '//td[contains(text(),"BENEFICIARY")]/ancestor::tr[1]/td[1]//*[contains(@style,"text-decoration")]//text()'
        b_address = '//td[contains(text(),"BENEFICIARY")]/ancestor::tr[1]/td[3]/address[descendant-or-self::text()]'
        beneficiary_name = html.xpath(b_name).get('')
        beneficiary_address = html.xpath(b_address).get('').replace('<address>', '').replace('</address>', '')

        petitions_dic = {'pet_1_name': '', 'pet_1_address': '', 'pet_2_name': '', 'pet_2_address': ''}
        index = 1
        for defendant in html.css('#c_PartiesPanel tbody tr'):
            key_value = ' '.join(''.join(defendant.css('td:nth-child(2) *::text').getall()).split())
            data_value_name = defendant.css('td:nth-child(1) [style*="underline"]::text').get()
            data_value_address = ' '.join(' '.join(defendant.css('td:nth-child(3) address').getall()).split())
            if 'PETITIONER' in key_value and index < 3:
                petitions_dic[f'pet_{index}_name'] = data_value_name
                petitions_dic[f'pet_{index}_address'] = data_value_address
                index += 1

        defendant_fname, defendant_lname = self.first_last_name(defendant_name)
        petition1_fname, petition1_lname = self.first_last_name(petitions_dic['pet_1_name'])
        petition2_fname, petition2_lname = self.first_last_name(petitions_dic['pet_2_name'])
        beneficiary_fname, beneficiary_lname = self.first_last_name(beneficiary_name)
        d_street_address, d_city, d_zipcode, d_state = self.city_zip(defendant_address)
        # d_city = self.find_city(defendant_address)
        # d_street_address = d_street_address.replace(d_city, '')

        b_street_address, b_city, b_zipcode, b_state = self.city_zip(beneficiary_address)
        # b_city = self.find_city(beneficiary_address)
        # b_street_address = b_street_address.replace(b_city, '')

        p1_street_address, p1_city, p1_zipcode, p1_state = self.city_zip(petitions_dic['pet_1_address'])
        # p1_city = self.find_city(petitions_dic['pet_1_address'])
        # p1_street_address = p1_street_address.replace(p1_city, '')

        p2_street_address, p2_city, p2_zipcode, p2_state = self.city_zip(petitions_dic['pet_2_address'])
        # p2_city = self.find_city(petitions_dic['pet_2_address'])
        # p2_street_address = p2_street_address.replace(p2_city, '')
        yield {
            'File Date': file_date,
            'Case Number': case_num,
            'Case Type': response.meta['case_type'],
            'Case Status': case_status,
            'Decedent First Name': defendant_fname,
            'Decedent Last Name': defendant_lname.strip(),
            'Decedent Street': ' '.join(d_street_address.split()),
            'Decedent City': d_city,
            'Decedent State': d_state,
            'Decedent Zipcode': d_zipcode,
            'Petition 1 First Name': petition1_fname,
            'Petition 1 Last Name': petition1_lname.strip(),
            'Petition 1 Street': ' '.join(p1_street_address.split()),
            'Petition 1 City': p1_city,
            'Petition 1 State': p1_state,
            'Petition 1 Zipcode': p1_zipcode,
            'Petition 2 First Name': petition2_fname,
            'Petition 2 Last Name': petition2_lname.strip(),
            'Petition 2 Street': ' '.join(p2_street_address.split()),
            'Petition 2 City': p2_city,
            'Petition 2 State': p2_state,
            'Petition 2 Zipcode': p2_zipcode,
            'Beneficiary First Name': beneficiary_fname,
            'Beneficiary Last Name': beneficiary_lname.strip(),
            'Beneficiary Street': ' '.join(b_street_address.split()),
            'Beneficiary City': b_city,
            'Beneficiary State': b_state,
            'Beneficiary Zipcode': b_zipcode,
        }

    def city_zip(self, address_string):
        address = address_string.replace('<address>', '').replace('</address>', '').split('<br>')
        city_state_zip = address[-1].strip().split(',')
        state_zip = ''.join(city_state_zip[-1])
        return ' '.join(address[:-1]), city_state_zip[0], state_zip.strip()[2:], state_zip.strip()[:2]

    def first_last_name(self, name):
        name = HumanName(name)
        return name.first, name.last
