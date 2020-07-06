"""
Scrapes all resource types from AWS documentation.

How to run::

    scrapy runspider scraper.py -o resource_types.json

Output will be a JSON file::

    [
    {"service": "Amazon AppFlow", "resource_type": "flow", "arn_format": "arn:${Partition}:appflow::${Account}:flow/${flowName}"},
    {"service": "Amazon AppFlow", "resource_type": "connectorprofile", "arn_format": "arn:${Partition}:appflow::${Account}:connectorprofile/${profileName}"},
    ...
    ]
"""

import json
import scrapy


class ResourceTypeSpider(scrapy.Spider):
    name = 'tocspider'
    start_urls = ['https://docs.aws.amazon.com/IAM/latest/UserGuide/toc-contents.json']
    download_delay = 0.250

    def parse(self, response):
        j = json.loads(response.text)
        j = j['contents']

        def select_title(xs, title):
            return [x for x in xs if x['title'] == title][0]

        j = select_title(j, 'Reference')['contents']
        j = select_title(j, 'Policy Reference')['contents']
        j = select_title(j, 'Actions, Resources, and Condition Keys')['contents']

        for page in j:
            title = page['title']
            href = page['href']
            print(title, href)
            yield response.follow(href, self.parse_ref)

            # break

    def parse_ref(self, response):
        service = response.xpath('//h2[starts-with(text(),"Resource Types Defined by")]/text()').get().replace("Resource Types Defined by", "").strip()

        trs = response.xpath("//th[text()='Resource Types']/../../..//tr")[1:]

        for tr in trs:
            tds = tr.xpath('td')
            row = []
            for td in tds:
                text = "".join([x.get().strip() for x in td.xpath(".//text()")])
                row.append(text)

            yield {'service': service, 'resource_type': row[0], 'arn_format': row[1]}
