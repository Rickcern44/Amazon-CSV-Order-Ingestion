# This program was written as a way to ingest amazon purchases into a database

---

## Steps for using this program

1. Fill in the config.ini file with your information.
2. Run the program

In order to use this program you will have to add a config.ini file and fill it in with the correct information

The file should look like this:

[Sender]<br>
Email = Email address that you want to send the alerts from.<br>
Password = Password for the above email.<br>

[Receiving]<br>
Email = Email address that you want to send the alerts to.<br>
<br>
[LocalDatabase]<br>
Server = 'Server name i.e localhost\\sqlexpess <br>
Database = DB Name <br>
<br>
[AWSDatabase]<br>
Server = For AWS this will be the endpoint<br>
Port = The port the server is on<br>
Username = "Username"<br>
Password = "Password"<br>

## For now this is the most effecient way for me to set this up to be used on different sytems.

In the future I plan on adding support for more databases and providers but for now it is just AWS.

## Currently the program is set up to create a log file the first time it is run. You can change the path of the log in the email service.py script

---

## Notes

1. Ensure that you put a .gitignore file for the config file if you plan on using version control I do not having in place to prevent you from uploading that. There is sensitive info in that file you do not want on Github.
2. I used this python script on a server that runs a weekly cron job and pulls from a network folder. Then into the databse and on to an API from there.
3. I found no way to automate getting the file from amazon to a folder on the desired machine. Amazon does not allow scraping on that portion of there site.
