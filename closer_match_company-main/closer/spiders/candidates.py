import scrapy
import pandas as pd

class CandidatesSpider(scrapy.Spider):
    name = "candidates"
    allowed_domains = ["find-and-update.company-information.service.gov.uk"]

    def __init__(self, file='candidates_list.csv'):
        self.df = pd.read_csv(file)
        self.urls = [(f'https://find-and-update.company-information.service.gov.uk/search?q=dr+{x}+{y}', str(x).lower(), str(y).lower(),z) for x, y, z in self.df[['First Name', 'Last Name','Year of Qualification']].values]
        
    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url=url[0], callback=self.parse, cb_kwargs={'firstname': url[1], 'lastname': url[2], 'qualification_year':url[3]})
    
    def parse(self, response, firstname, lastname, qualification_year):
        baselink = 'https://find-and-update.company-information.service.gov.uk'
        for li in response.css('li.type-officer'):

            name = li.css('a.govuk-link::text').get().lower().replace('dr', '').split()
            birthdate = li.xpath('p[1]/text()').get()


            if name[0] == firstname and name[-1] == lastname and birthdate[-4:].isnumeric():

                if (int(qualification_year)-int(birthdate[-4:])) in [22,23,24,25,26]:
                    # address = li.xpath('p[2]/text()').get()

                    link = li.css('a.govuk-link::attr(href)').get()
                    yield scrapy.Request(baselink + link, callback = self.parse_candidate, cb_kwargs={'name':name, 'qualification_year':qualification_year, 'birthdate':birthdate, 'baselink':baselink})

    def parse_candidate(self, response, name, qualification_year, birthdate, baselink):
        for i in range(int(response.css('#personal-appointments::text').get().strip()[-1])):
            if response.css(f"#occupation-value-{i+1}::text").get() is not None:
                profession = response.css(f"#occupation-value-{i+1}::text").get().strip()
            else:
                profession = None
            appointed_on = response.css(f"#appointed-value{i+1}::text").get().strip()
            
            company_link = response.css('div.appointment-1').css('a::attr(href)').get()
            yield scrapy.Request(baselink + company_link, callback = self.parse_company, cb_kwargs={'name':name, 'qualification_year':qualification_year, 'birthdate':birthdate,'profession':profession,'appointed_on':appointed_on})
    
    def parse_company(self, response, name, qualification_year, birthdate,profession,appointed_on): 
        if response.css('#sic0::text').get() is not None:
            Nature_of_business = response.css('#sic0::text').get().strip()
        else:
            Nature_of_business = None 

            
        yield {
            'name': (" ").join(name),
            'qualification_year': qualification_year,
            'borndate': birthdate[-4:],
            'profession': profession,
            'appointed_on': appointed_on,
            'company_name': response.css('p.heading-xlarge::text').get(),
            'address': response.css('div.govuk-tabs__panel').css('dd::text').get().strip(),
            'company_status': response.css('#company-status::text').get().strip(),
            'Nature_of_business': Nature_of_business
        } 

