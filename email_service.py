import smtplib
import configparser

# Adding a comment to make the Dev branch
# get the variables from the config file so that they can be changed later


def get_email_info():
    config = configparser.ConfigParser()
    config.read('devconfig.ini')

    # Get sender email
    sender_email = config['Sender']['Email']

    # Get sender password
    sender_pass = config['Sender']['Password']

    # Get Receving email
    receiving_email = config['Receiving']['Email']
    return sender_email, sender_pass, receiving_email


email_info = get_email_info()


def send_success_email(record_count, time_taken):
    smtpObj = smtplib.SMTP('smtp-mail.outlook.com', 587)
    smtpObj.ehlo()
    smtpObj.starttls()

    # Login to the outlook account
    smtpObj.login(email_info[0], email_info[1])

    # Send the email
    message = f'''Subject: EBM Amazon Ingest Stats \n
        During todays ingestion there were {record_count} new records \n
        The total time for Ingest was {time_taken} seconds.
        Thank you for Testing EBM
        EBM Development Team'''
    smtpObj.sendmail(email_info[0],
                     email_info[2], message)

    # Close the connection
    smtpObj.quit()


# If the ingestion fails send the reason why it failed. Figure out global varibales needed for this
def send_ingest_failure_email():
    smtpObj = smtplib.SMTP('smtp-mail.outlook.com', 587)
    smtpObj.ehlo()
    smtpObj.starttls()

    # Login to the outlook account
    smtpObj.login(email_info[0], email_info[1])

    # Send the email
    message = f'''Subject: EBM Amazon Ingest Status \n
        There was an issue with the file. It was either not put in the landing zone or there was an ingest error.\n
        Check the server log file for more info on the error. Please run the job manually to make sure the new purchases are added to the database.\n
        \n
        Thank you for Testing EBM\n
        EBM Development Team'''
    smtpObj.sendmail(email_info[0],
                     email_info[2], message)

    # Close the connection
    smtpObj.quit()
