from celery import Celery
import celeryconfig
import argparse
import cv2
import sys
import numpy as np
import core.utils as utils
import tensorflow as tf
from core.yolov3 import YOLOv3, decode
from core.config import cfg
from PIL import Image
import requests
import base64
import io
from io import BytesIO
import datetime
import time

app = Celery('worker', broker='redis://localhost:6379/0')
app.config_from_object(celeryconfig)

@app.task
def process_frame(vid_name, frame_num, image):
    print("processing frame "+str(frame_num))
    exit()
    start_time = datetime.datetime.now()
    #Decode received image
    image = base64.decodebytes(image.encode('ascii'))
    image = Image.open(io.BytesIO(image))
    original_image = np.array(image)

    # Read class names
    class_names = {}
    with open(cfg.YOLO.CLASSES, 'r') as data:
        for ID, name in enumerate(data):
            class_names[ID] = name.strip('\n')

    # Setup tensorflow, keras and YOLOv3
    input_size   = 416
    input_layer  = tf.keras.layers.Input([input_size, input_size, 3])
    feature_maps = YOLOv3(input_layer)

    original_image      = cv2.cvtColor(original_image, cv2.COLOR_BGR2RGB)
    original_image_size = original_image.shape[:2]

    image_data = utils.image_preporcess(np.copy(original_image), [input_size, input_size])
    image_data = image_data[np.newaxis, ...].astype(np.float32)

    bbox_tensors = []
    for i, fm in enumerate(feature_maps):
        bbox_tensor = decode(fm, i)
        bbox_tensors.append(bbox_tensor)

    model = tf.keras.Model(input_layer, bbox_tensors)
    utils.load_weights(model, "./yolov3.weights")

    pred_bbox = model.predict(image_data)
    pred_bbox = [tf.reshape(x, (-1, tf.shape(x)[-1])) for x in pred_bbox]
    pred_bbox = tf.concat(pred_bbox, axis=0)

    bboxes = utils.postprocess_boxes(pred_bbox, original_image_size, input_size, 0.3)
    bboxes = utils.nms(bboxes, 0.45, method='nms')

    # We have our objects detected and boxed, lets move the class name into a list
    person_count = 0
    objects_detected = []
    for x0,y0,x1,y1,prob,class_id in bboxes:
        if class_names[class_id]=="person":
            person_count+=1
        objects_detected.append(class_names[class_id])
    
    end_time = datetime.datetime.now() - start_time
    
    return_json={
        "vid_name":vid_name,
        "frame_num":frame_num,
        "process_time":str(end_time),
        "person_count":person_count,
        "detected_objects":objects_detected
    }
    requests.post("http://localhost:5000/returnframe", json=return_json)

    return ("PERSON COUNT: "+str(person_count))
