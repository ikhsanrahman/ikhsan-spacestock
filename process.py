import json
import pandas as pd
import time
import csv

from pymongo import MongoClient
from sqlalchemy import create_engine
from flask import Flask
from flask_sqlalchemy import SQLAlchemy 
from tqdm import tqdm

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://stock:stock@localhost/stockmarket"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


config = json.load(open("config/config.json", "r", encoding='utf-8'))
mongo = MongoClient(config['MONGODB_URI'])

print("Running the initial script....")


#get data from database mongodb
def get_data_mongo(collection):
    """
    The function to query the data from mongodb.
    """
    try:
        if not collection:
            collection = "complex"
        db = mongo[config['DATABASE']]
        data = db[collection].find()
        return list(data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(e)


# define tables in postgresql db
class Complex(db.Model):
    """
    Class the related to table complex
    """
    complex_id              = db.Column(db.String(255), primary_key=True)
    isActive                = db.Column(db.Boolean, default=False)
    Category                = db.Column(db.String(255))
    Name                    = db.Column(db.String(255))


class Tower(db.Model):
    """
    Class the related to tower complex
    """
    tower_id                = db.Column(db.String(255), primary_key=True)
    isActive                = db.Column(db.Boolean, default=False)
    Category                = db.Column(db.String(255))
    Name                    = db.Column(db.String(255))


class Unit(db.Model):
    """
    Class the related to unit complex
    """
    unit_id                 = db.Column(db.String(255), primary_key=True)
    isActive                = db.Column(db.Boolean, default=False)
    Category                = db.Column(db.String(255))
    Size                    = db.Column(db.Numeric(precision=8, asdecimal=True, decimal_return_scale=None))


class ImageVideo(db.Model):
    """
    Class that will handle the amout of certain data
    """
    id                      = db.Column(db.String(255), primary_key=True)
    name                    = db.Column(db.String(255))
    images_developer        = db.Column(db.Integer, default=0)
    images_banner           = db.Column(db.Integer, default=0)
    images_brochure         = db.Column(db.Integer, default=0)
    images_interior         = db.Column(db.Integer, default=0)
    images_exterior         = db.Column(db.Integer, default=0)
    images_360              = db.Column(db.Integer, default=0)
    video_link              = db.Column(db.Integer, default=0)
   

print("Setting up the database...")
for _ in range(5):
    time.sleep(0.1)
    print("...")
# create db in postgresql
db.create_all()

complexs = get_data_mongo("complex")
towers = get_data_mongo("tower")
complexs_towers = []
complexs_towers.extend(complexs)
complexs_towers.extend(towers)

print('Starting to migrate the database from mongo to postgresql...')
data_complex = tqdm(complexs)
# process the data from complex collection
for data in data_complex:
    name = data.get("name")
    data_complex.set_description(f"process {name} of {len(data_complex)} data complex")
    time.sleep(0.05)
    # check the data if exists or not
    check_data = Complex.query.filter_by(complex_id=data["id"]).first()
    if not check_data:
        if not data.get('status'):
            isActive = False
        elif data['status'] is 0:
            isActive = False
        elif data['status'] is 1:
            isActive = True
        new_data = Complex(complex_id=data["id"], isActive=isActive, Category=data['category'], Name=data['name'])
        db.session.add(new_data)
        db.session.commit()
time.sleep(0.2)
print("new data for complex table has been stored successfully")

for _ in range(2):
    time.sleep(0.1)
    print("...")

data_tower = tqdm(towers)
# process the data from tower collection
for data in data_tower:
    name = data.get("name")
    data_tower.set_description(f"process {name} of {len(data_complex)} data tower")
    time.sleep(0.01)
    # check the data if exists or not
    check_data = Tower.query.filter_by(tower_id=data["id"]).first()
    if not check_data:
        if not data.get('status'):
            isActive = False
        elif data['status'] is 0:
            isActive = False
        elif data['status'] is 1:
            isActive = True
        new_data = Tower(tower_id=data["id"], isActive=isActive, Category=data['category'], Name=data['name'])
        db.session.add(new_data)
        db.session.commit()
time.sleep(0.1)
print("new data for tower table has been stored successfully")

for _ in range(2):
    time.sleep(0.1)
    print("...")

data_unit = tqdm(complexs)
# process the data from complex collection
for data in data_unit:
    name = data.get("name")
    data_unit.set_description(f"process {name} of {len(data_unit)} data unit")
    time.sleep(0.01)
    # check the data if exists or not
    check_data = Unit.query.filter_by(unit_id=data["id"]).first()
    if not check_data:
        if not data.get('status'):
            isActive = False
        elif data['status'] is 0:
            isActive = False
        elif data['status'] is 1:
            isActive = True
        new_data = Unit(unit_id=data["id"], isActive=isActive, Category=data['category'], Size=data['land_size'])
        db.session.add(new_data)
        db.session.commit()
time.sleep(0.1)
print("new data for unit has been stored successfully")

for _ in range(2):
    time.sleep(0.1)
    print("...")

print("Starting to count amount of image and video")
# concatenate for towers's data and complex's data
data_complex_tower = tqdm(complexs_towers)
for data in data_complex_tower:
    name = data.get("name")
    
    data_complex_tower.set_description(f"process counting {name} of {len(data_complex_tower)} data tower and complex")
    time.sleep(0.01)
    check_data_tower = Tower.query.filter_by(tower_id=data["id"]).first()
    check_data_complex = Complex.query.filter_by(complex_id=data["id"]).first()
    check_image_video = ImageVideo.query.filter_by(id=data["id"]).first()
    if not check_image_video and (check_data_tower or check_data_complex):
        if data.get("images").get("images_developer") :
            total = len(data.get("images").get("images_developer"))
            if total >= 1 and total <= 2:
                image_dev = 5
            elif total >= 3 and total <= 4:
                image_dev = 7
            elif total >= 5:
                image_dev = 10
            else:
                image_dev = 0
        if not data.get("images").get("images_developer"):
            image_dev = 0

        if data.get("images").get("images_banner"):
            total = len(data.get("images").get("images_banner"))
            # print(total, "ban")
            if total >= 1 and total <= 2:
                image_banner = 10
            elif total >= 3 and total <= 4:
                image_banner = 10
            elif total >= 5:
                image_banner = 10
            else:
                image_banner = 0
        if not data.get("images").get("images_banner"):
            image_banner = 0

        if data.get("images").get("images_brochure"):
            total = len(data.get("images").get("images_brochure"))
            # print(total, "bro")
            if total >= 1 and total <= 2:
                image_broc = 7
            elif total >= 3 and total <= 4:
                image_broc = 10
            elif total >= 5:
                image_broc = 10
            else:
                image_broc = 0
        if not data.get("images").get("images_brochure"):
            image_broc = 0

        if data.get("images").get("images_interior"):
            total = len(data.get("images").get("images_interior"))
            # print(total, "int")
            if total >= 1 and total <= 2:
                image_int = 5
            elif total >= 3 and total <= 4:
                image_int = 7
            elif total >= 5:
                image_int = 10
            else:
                image_int = 0
        if not data.get("images").get("images_interior"):
            image_int = 0

        if data.get("images").get("images_exterior"):
            total = len(data.get("images").get("images_exterior"))
            # print(total, "ext")
            if total >= 1 and total <= 2:
                image_ext = 3
            elif total >= 3 and total <= 4:
                image_ext = 5
            elif total >= 5:
                image_ext = 10
            else:
                image_ext = 0
        if not data.get("images").get("images_exterior"):
            image_ext = 0

        if data.get("images").get("images_360"):
            total = len(data.get("images").get("images_360"))
            # print(total, "360")
            if total >= 1 and total <= 2:
                image_360 = 7
            elif total >= 3 and total <= 4:
                image_360 = 10
            elif total >= 5:
                image_360 = 10
            else:
                image_360 = 0
        if not data.get("images").get("images_360"):
            image_360 = 0         

        if data.get("images").get("video_link"):
            total = len(data.get("images").get("video_link"))
            # print(total, "vid")
            if total >= 1 and total <= 2:
                video_link = 10
            elif total >= 3 and total <= 4:
                video_link = 10
            elif total >= 5:
                video_link = 10
            else:
                video_link = 0
        if not data.get("images").get("video_link"):
            video_link = 0

        new_amount = ImageVideo(id=data["id"], name=data['name'], images_developer=image_dev, images_banner=image_banner, \
            images_brochure=image_broc, images_interior=image_int, images_exterior=image_ext, images_360=image_360, \
            video_link=video_link)
        db.session.add(new_amount)
        db.session.commit()
    
for _ in range(2):
    time.sleep(0.1)
    print("...")
print("counting amount of image and video has already done successfully")


for _ in range(2):
    time.sleep(0.1)
    print("...")
print("Export all table in progresql to csv file")
complex_csv = "complex.csv"
tower_csv = "tower.csv"
unit_csv = "unit.csv"
image_video_csv = "img_video.csv"

# Create csv file for complex table in postgresql
with open(complex_csv, 'w', newline='') as f:
    writer =csv.writer(f, delimiter=",")
    writer.writerow(["Complex ID", "Is Active", "Category", "Name"])
    data = Complex.query.all()
    for item in data:
        writer.writerow([item.complex_id, item.isActive, item.Category, item.Name])

# Create csv file for tower table in postgresql
with open(tower_csv, 'w', newline='') as f:
    writer =csv.writer(f, delimiter=",")
    writer.writerow(["Tower ID", "Is Active", "Category", "Name"])
    data = Tower.query.all()
    for item in data:
        writer.writerow([item.tower_id, item.isActive, item.Category, item.Name])

# Create csv file for unit table in postgresql
with open(unit_csv, 'w', newline='') as f:
    writer =csv.writer(f, delimiter=",")
    writer.writerow(["Unit ID", "Is Active", "Category", "Size"])
    data = Unit.query.all()
    for item in data:
        writer.writerow([item.unit_id, item.isActive, item.Category, item.Size])

# Create csv file for result of amount data of image and video in postgresql
with open(image_video_csv, 'w', newline='') as f:
    writer =csv.writer(f, delimiter=",")
    writer.writerow(["ID", "Name", "Images Developer", "Images Banner", "Images Brochure", "Images Interior", "Images Exterior",\
        "Images 360", "Video Link"])
    data = ImageVideo.query.all()
    for item in data:
        writer.writerow([item.id, item.name, item.images_developer, item.images_banner, item.images_brochure, item.images_interior,\
            item.images_exterior, item.images_360, item.video_link])



for _ in range(5):
    time.sleep(0.2)
    print("...")
print("Processing done")





