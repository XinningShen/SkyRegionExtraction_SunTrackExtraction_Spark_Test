import sys
import ImagePullerHelper


if __name__ == "__main__":
    try:
        deviceid = sys.argv[1]
        start_date = sys.argv[2]
        start_time = sys.argv[3]
        end_date = sys.argv[4]
        end_time = sys.argv[5]
        file_path = sys.argv[6]
    except:
        print("Wrong Input!")

    validated = ImagePullerHelper.validate(deviceid, start_date, start_time, end_date, end_time)

    if validated[0] == 'ERR':
        print "Device ID is wrong!"
        sys.exit()

    data = ImagePullerHelper.make_corrected_epoch(validated[0], validated[1], validated[2], validated[3], validated[4])

    urls = ImagePullerHelper.db_query(data)

    if urls == []:
        print "No URL"
        sys.exit()

    ImagePullerHelper.pull_and_zip(urls, data, file_path)