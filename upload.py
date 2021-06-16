"""
Created by Sara Geleskie Damiano
"""
#%%
from os import times
import sys
import time
import copy
import re
import io

import requests
from requests import Request, Session
from bs4 import BeautifulSoup

import pandas as pd

# Same as the "Site Code" of your station at MonitorMyWatershed.org/site/_____
upload_site = " "

# Copy the name of the file you uploaded into the Binder folder. Be sure to add ./ to the beginning and .csv at the end
upload_file = "./ .csv"

# Get this from your MMW.org station page's "Token and UUID List," or from the CSV file header's "Sampling Feature UUID"
feature_uuid = " "

# Your time zone offset (-5 is EST)
tz_offset = -5

# Your username
mmw_user = " "

# Your password
mmw_pass = " "

mmw_host = "https://monitormywatershed.org"

login_page = "{}/login/".format(mmw_host)

#%% Authenticate with MMW
s = Session()
s.verify = True


def print_headers(headers):
    out_str = ""
    for header in headers:
        out_str += "\t{}: {}\n".format(header, headers[header])
    return out_str


def print_req(the_request):
    print_format = "\nRequest:\nmethod: {}\nurl: {}\nheaders:\n{}\nbody: {}\n\nResponse:\nstatus code: {}\nurl: {}\nheaders: {}\ncookies: {}\nbody: {}"
    print(
        print_format.format(
            the_request.request.method,
            the_request.request.url,
            print_headers(the_request.request.headers),
            the_request.request.body,
            the_request.status_code,
            the_request.url,
            print_headers(the_request.headers),
            the_request.cookies,
            the_request.content,
        )
    )


def print_req_trace(the_request):
    if the_request.history:
        print("\nRequest was redirected")
        for resp in the_request.history:
            print_req(resp)
        print("\n\nFinal destination:")
        print_req(the_request)
    else:
        print_req(the_request)


s.headers.update(
    {
        "Host": "monitormywatershed.org",
        # "Authorization": mmw_api_key,
        # "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0",
        "Accept": "*/*",
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        # "Pragma": "no-cache",
        "Origin": "https://monitormywatershed.org",
    }
)

# Retrieve the CSRF token
crsf_req2 = s.get(
    login_page,
    # params={"next": "/api/docs/"},
    headers={"Referer": mmw_host,},
)
# print_req(crsf_req2)
soup = BeautifulSoup(crsf_req2.content)
csrf_middle_token2 = soup.find("input", dict(name="csrfmiddlewaretoken"))["value"]
print("CSRF Middleware Token: {}\n".format(csrf_middle_token2))

# construct the auth payload
auth_payload = {
    "csrfmiddlewaretoken": (None, csrf_middle_token2),
    "next": (None, "/"),
    "username": (None, mmw_user),
    "password": (None, mmw_pass),
}

# log in
login = s.post(login_page, files=auth_payload, headers={"Referer": login_page})
cookie_dict = requests.utils.dict_from_cookiejar(login.cookies)
# print_req_trace(login)
print("\nSession cookies: {}".format(s.cookies))

#%% get the site page in order to get the upload link
page_req = s.get("{}/sites/update/{}/sensors/".format(mmw_host, upload_site))
# print_req_trace(page_req)
soup = BeautifulSoup(page_req.content)
upload_link = soup.find("form", id="form-file-upload")["action"]
upload_token = soup.find("input", dict(name="csrfmiddlewaretoken"))["value"]

# %% Read in the data file header
reg = re.compile("^[0-9\.: ]")
first_read = open(upload_file)
header = ""
n_headers = 0
header_lines = []
skip_lines = []
with open(upload_file) as f:
    for line in f:
        if bool(reg.search(line)):
            break
        if "Result UUID:" in line:
            header_lines.append(n_headers)
        elif "Sampling Feature UUID: " in line:
            feature_uuid = line.replace("Sampling Feature UUID: ", "").replace("\n", "")
            print("Got feature UUID of {}".format(feature_uuid))
            skip_lines.append(n_headers)
        else:
            print("Ignoring header {}".format(line))
            skip_lines.append(n_headers)
        header += line
        n_headers += 1

#%% read in the data
full_read = pd.read_csv(
    upload_file,
    header=0,
    skiprows=skip_lines,
    parse_dates=True,
    index_col="Result UUID:",
)
reader = pd.read_csv(
    upload_file,
    header=0,
    skiprows=skip_lines,
    parse_dates=True,
    index_col="Result UUID:",
    chunksize=2000,
)
chunk_num = 0
for chunk in reader:
    chunk_num += 1
    print(
        "Uploading chunk {} with first date: {}, last date: {}".format(chunk_num,
            chunk.index.min(), chunk.index.max()
        )
    )
    if "Unnamed: 1" in chunk.columns:
        chunk = chunk.drop(columns=["Unnamed: 1"])
    if "Unnamed: 2" in chunk.columns:
        chunk = chunk.drop(columns=["Unnamed: 2"])
    csv_string = "Sampling Feature UUID: {}\n".format(feature_uuid)
    csv_string += "Date and Time in UTC{}\n".format(tz_offset)
    csv_string += chunk.to_csv()
    upload_request = Request(
        "POST",
        url="{}{}".format(mmw_host, upload_link),
        files={
            "data_file": ("data_file.txt", csv_string, "text/plain"),
            "csrfmiddlewaretoken": (None, upload_token),
        },
        headers={
            "Referer": "{}/sites/update/{}/sensors/".format(mmw_host, upload_site),
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    prepped = s.prepare_request(upload_request)
    # print(
    #     "\nRequest:\nmethod: {}\nurl: {}\nheaders:\n{}\nbody:\n{}".format(
    #         prepped.method,
    #         prepped.url,
    #         print_headers(prepped.headers),
    #         prepped.body.decode("utf-8"),
    #     )
    # )
    file_post = s.send(prepped, timeout=180)
    # print_req_trace(file_post)
    print("  Result: {}".format(file_post.text))
    # break

# %%
