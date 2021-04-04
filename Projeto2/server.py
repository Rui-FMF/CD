from flask import Flask
from flask import request
from worker import process_frame
import argparse
import cv2
import sys
from PIL import Image
import requests
import base64
import io
from io import BytesIO

server = Flask(__name__)


max_people = 10
vids = {}

@server.route("/", methods=['POST'])
def index():
    vid_name = request.form['video']
    vids[vid_name] = {"total_frames":0,"processed_frames":0, "person_count":0, "total_time":0.0, "all_obj":[]}
    vidcap = cv2.VideoCapture(vid_name)
    success,image = vidcap.read()
    count = 1
    while (success and count <33):
        #Encoding the image to be able to send it
        image = Image.fromarray(image)
        image_bytes = io.BytesIO()
        image.save(image_bytes, format='JPEG')
        image_bytes = image_bytes.getvalue()
        encoded_image = base64.encodebytes(image_bytes).decode('ascii')
        #calling celery task
        process_frame.delay(vid_name, count, encoded_image)
        success,image = vidcap.read()
        count += 1

    vids[vid_name]["total_frames"]=(count-1)
    print("Video sent for processing!")
    return "Video sent for processing!\n"

@server.route("/returnframe", methods=['POST'])
def returnframe():
    frame_info = request.get_json()

    #Warn if the frame as exceding ammount of people
    if frame_info["person_count"] > max_people:
        print('\033[93m'+"WARNING Video"+frame_info["vid_name"]+" has number of people above the limit on Frame "+str(frame_info["frame_num"])+'\033[0m')

    #Update video info
    vid_name = frame_info["vid_name"]
    time = frame_info["process_time"]
    time_in_ms = float(time.split(":")[0])*3600000 + float(time.split(":")[1])*60000 + float(time.split(":")[2])*1000

    vids[vid_name]["processed_frames"]+=1
    vids[vid_name]["person_count"]+= frame_info["person_count"]
    vids[vid_name]["total_time"]+= time_in_ms
    vids[vid_name]["all_obj"]+=frame_info["detected_objects"]

    #Print ammount of people in the frame
    print('\033[92m'+"Frame "+str(frame_info["frame_num"])+": "+str(frame_info["person_count"])+" <person> detected in "+ str(int(time_in_ms))+'\033[0m')
    
    #Compute final statistics

    if (vids[vid_name]["total_frames"] == vids[vid_name]["processed_frames"]):

        print('\n')
        print('\033[94m'+"Processing final video information...")
        unique_obj = []
        obj_dic = {}
       
        for x in vids[vid_name]["all_obj"]: 
            if x not in unique_obj: 
                unique_obj.append(x)
        
        for y in unique_obj:
            occurences = vids[vid_name]["all_obj"].count(y)
            obj_dic[y] = occurences

        ordered_dic = {k: v for k, v in sorted(obj_dic.items(), key=lambda item: item[1])}
        ordered_list = list(ordered_dic.keys())
        ordered_list.reverse()

        print("Processed Frames: "+str(vids[vid_name]["processed_frames"]))
        print("Average processing time per frame: "+str(int( vids[vid_name]["total_time"]/vids[vid_name]["processed_frames"] )))
        print("Person objects detected: "+str(vids[vid_name]["person_count"]))
        print("Total classes detected: "+str(len(unique_obj)))
        print("Top 3 objects detected: "+ordered_list[0]+" "+ ordered_list[1]+" "+ordered_list[2]+" "+'\033[0m')
        print('\n')
    
    return "Image received!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", help="maximum number of persons in a frame", default=10)
    args = parser.parse_args()

    max_people = int(args.max)
    server.run()



