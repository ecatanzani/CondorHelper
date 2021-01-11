#!/usr/bin/python3

from argparse import ArgumentParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

def ParseConfigFile(input_config: str) -> dict:
    # Read config file
    try:
        config_params = []
        with open(input_config, "r") as _config:
            for line in _config:
                for word in line.split():
                    config_params.append(word)
    except:
        print("Mail config missing...")
        raise

    """Compose and send email with provided info and attachments.

			Args:
			send_from (str): from name
			send_to (str): to name
			server (str): mail server host name
			port (int): port number
			username (str): server auth username
			password (str): server auth password
			use_tls (bool): use TLS mode
			"""

    dConfig = {'send_from': "", 'send_to': "", 'server': "",
               'port': 0, 'username': "", 'password': "", 'use_tls': True}

    for idx, word in enumerate(config_params):
        if word == "send_from":
            dConfig['send_from'] = config_params[idx+1]
        if word == "send_to":
            dConfig['send_to'] = config_params[idx+1]
        if word == "server":
            dConfig['server'] = config_params[idx+1]
        if word == "port":
            dConfig['port'] = config_params[idx+1]
        if word == "username":
            dConfig['username'] = config_params[idx+1]
        if word == "password":
            dConfig['password'] = config_params[idx+1]
        if word == "use_tls":
            dConfig['use_tls'] = config_params[idx+1]

    return dConfig


def ShowConfigInfo(pars: dict):
    print("\n**** Listing mail config parameters (KEEP EM PRIVATE!) ****\n")
    for key in pars:
        print(key, pars[key])
    print("\n***************\n")


def DeployMail(pars: dict):
    
    _subject = "CNAF jobs completed"
    _message = "All submitted jobs have been completed"

    print("Preparing Mail...")

    msg = MIMEMultipart()
    msg['From'] = pars["send_from"]
    msg['To'] = pars["send_to"]
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = _subject

    msg.attach(MIMEText(_message))

    print("Connecting to server...")
    try:
        smtp = smtplib.SMTP(pars["server"], pars["port"])
        if pars["use_tls"]:
            smtp.starttls()
        smtp.login(pars["username"], pars["password"])
    except:
        print("Error connecting to privider server...")
        raise

    print("Sending...")
    smtp.sendmail(pars["send_from"], pars["send_to"], msg.as_string())
    print("Mail sent")
    smtp.quit()


def main(args=None):
    parser = ArgumentParser(
        usage="Usage: %(prog)s [options]", description="Postman Mail Notifier")
    parser.add_argument("-c", "--config", type=str,
                        dest='config', help='mail config file')
    parser.add_argument("-s", "--show", dest='show', default=False,
                        action='store_true', help='show mail config file')

    opts = parser.parse_args(args)

    _pars = ParseConfigFile(opts.config)
    if (opts.show):
        ShowConfigInfo(_pars)
    DeployMail(_pars)


if __name__ == "__main__":
    main()
