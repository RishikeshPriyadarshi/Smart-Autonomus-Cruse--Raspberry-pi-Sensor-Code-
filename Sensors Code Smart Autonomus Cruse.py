import pymongo
import time
import RPi.GPIO as GPIO
import threading
from picamera2 import Picamera2, Preview
import board
import adafruit_dht

class Database_mongo:
    def __init__(self):
        
        # Connection_string = "mongodb+srv://user:User@major-project.8kkxvv2.mongodb.net/?retryWrites=true&w=majority"
        Connection_string = "mongodb+srv://Rish:User@majorprojectcluster.wtat6nb.mongodb.net/RawData?retryWrites=true&w=majority&appName=MajorProjectCluster"
        myclient = pymongo.MongoClient(Connection_string)
        db = myclient["RawData"]
        collist = db.list_collection_names()
        self.imageDataBase = db["imagedatas"]
        self.collection = db["sensor datas"]
        self.logColl = db["logdatas"]
        self.relayflag = True
        
        self.stop_threads = False
        
        self.lock = threading.Lock()  # Create a lock
        
        self.getRelay_thread = threading.Thread(target=self.getRelay)
        self.capture_thread = threading.Thread(target=self.capture_and_upload)
        self.moisture_thread = threading.Thread(target=self.getMoisture)
        self.capture_and_upload_thread = threading.Thread(target=self.capture_and_upload)
        self.humidity = threading.Thread(target=self.Humidity)
        self.morter = threading.Thread(target=self.getMorter)
        
        self.getRelay_thread.start()
        self.capture_thread.start()
        self.moisture_thread.start()
        self.capture_and_upload_thread.start()
        self.humidity.start()
        self.morter.start()
        
    def UpdateLogs(self, message):
        mylogs = {"message": message}
        result = self.logColl.find({"message": {"$exists": True}})
        for document in result:
            newVale = {"$set": {"message": message}}
            self.logColl.update_one({"_id": document["_id"]}, newValue);
        
    def insert(self):
        mydict = { "relay": True, "temprature": 56, "Moisture": 300 }
        x = self.collection.insert_one(mydict)
        print(x.acknowledged)
        return x.acknowledged
    
    def updateRelay(self, value):
        result = self.collection.find({"relay": {"$exists": True}})
        for document in result:
            newvalues = { "$set": { "relay": value } }
            self.collection.update_one({"_id": document["_id"]}, newvalues)
        print("Update completed")
        
    def getRelay(self):
        try:
            print("getRelay thread started")
            GPIO.setmode(GPIO.BCM)
            GPIO.setwarnings(False)
            Relay_pin = 14
            GPIO.setup(Relay_pin, GPIO.OUT)
        
            while not self.stop_threads:
                result = self.collection.find({"relay": {"$exists": True}})
                for x in result:
                    relay_state = x["relay"]
                if relay_state == True:
                    GPIO.output(Relay_pin, GPIO.LOW)
                    print("Grow light ON")
                else:
                    GPIO.output(Relay_pin, GPIO.HIGH)
                    print("Grow light OFF")
                time.sleep(2)
        except Exception as e:
            print("Exception in getRelay thread:", e)
        finally:
            print("getRelay thread exited")
            GPIO.cleanup()
    
    def getMorter(self):
        try:
            print("getMorter thread started")
            GPIO.setmode(GPIO.BCM)
            GPIO.setwarnings(False)
            Relay_pin = 22
            GPIO.setup(Relay_pin, GPIO.OUT)
        
            while not self.stop_threads:
                result = self.collection.find({"morterStart": {"$exists": True}})
                for x in result:
                    relay_state = x["morterStart"]
                if relay_state == True:
                    GPIO.output(Relay_pin, GPIO.LOW)
                    print("Mortar started")
                else:
                    GPIO.output(Relay_pin, GPIO.HIGH)
                    print("Mortar stopped")
                time.sleep(2)
        except Exception as e:
            print("Exception in getMorter thread:", e)
        finally:
            print("getMorter thread exited")
            GPIO.cleanup()
            
    def updateIdStr(self,val):
        val = str(val)
        result = self.collection.find({"imageIdStr": {"$exists": True}})
        ack = ""
        
        for document in result:
            newvalues = { "$set": { "imageIdStr": val } }
            ack = self.collection.update_one({"_id": document["_id"]}, newvalues)
            
        print("id in string updated")
            
    def capture_and_upload(self):
        try:
            print("capture_and_upload thread started")
            while not self.stop_threads:
                with self.lock:  # Acquire the lock
                    result = self.collection.find({"captureImage": {"$exists": True}})
                    temp=""
                    print("camera is closed")
                    for document in result:
                        temp = document["captureImage"]
                        if temp == True:
                            print("Capturing the image")
                            self.collection.update_one({"_id": document["_id"]}, {"$set": {"captureImage": False}})
                            picam2 = Picamera2()
                            camera_config = picam2.create_still_configuration(main={"size": (1920, 1080)}, lores={"size": (640, 480)}, display="lores")
                            picam2.configure(camera_config)
                            picam2.start_preview(Preview.QTGL)
                            picam2.start()
                            time.sleep(2)
                            image_file = "test.jpg"
                            picam2.capture_file(image_file)
                            with open(image_file, "rb") as f:
                                image_data = f.read()
                                
                                result = self.imageDataBase.insert_one({"image": image_data})
                                print("Image uploaded with ObjectID:", result.inserted_id)
                                self.updateIdStr(result.inserted_id)
                            picam2.stop()  # Stop the camera
                            picam2.stop_preview()
                            picam2.close()
                time.sleep(2)
        except Exception as e:
            print("Exception in capture_and_upload thread:", e)
        finally:
            print("capture_and_upload thread exited")
            
    def updateTemp(self, value):
        result = self.collection.find({"temprature": {"$exists": True}})
        ack = ""
        for document in result:
            newvalues = { "$set": { "temprature": value } }
            ack = self.collection.update_one({"_id": document["_id"]}, newvalues)
        print("Update completed")
        return ack.acknowledged
    
    def getTemp(self):
        result = self.collection.find({"temprature": {"$exists": True}})
        for x in result:
            temp = x["temprature"]
        return temp
    
    def updateMoisture(self, value):
        result = self.collection.find({"moisture": {"$exists": True}})
        ack = ""
        for document in result:
            newvalues = { "$set": { "moisture": value } }
            ack = self.collection.update_one({"_id": document["_id"]}, newvalues)
        ack.acknowledged
    
    def getMoisture(self):
        try:
            print("getMoisture thread started")
            moisture_pin = 21
            GPIO.setup(moisture_pin, GPIO.IN)
        
            while not self.stop_threads:
                moisture_val = GPIO.input(moisture_pin)
                self.updateMoisture(moisture_val)
                if(moisture_val == 1):
                    print("Need water")
                else:
                    print("Sufficient water")
                time.sleep(3)
        except Exception as e:
            print("Exception in getMoisture thread:", e)
        finally:
            print("getMoisture thread exited")
            GPIO.cleanup()
    
    def updateHumidity(self,humi):
        result = self.collection.find({"humidity": {"$exists": True}})
        ack = ""
        for document in result:
            newvalues = { "$set": { "humidity": humi } }
            ack = self.collection.update_one({"_id": document["_id"]}, newvalues)
        ack.acknowledged
        
    def updateTemperature(self,temp):
        result = self.collection.find({"temperature" : {"$exists": True}})
        for document in result:
            newvalues={"$set": {"temperature" : temp}}
            self.collection.update_one({"_id": document["_id"]},newvalues)
    
    def Humidity(self):
        try:
            print("Humidity thread started")
            dhtDevice = adafruit_dht.DHT11(board.D20)
            while not self.stop_threads:
                try:
                    temperature_c = dhtDevice.temperature
                    if temperature_c is not None:
                        temperature_f = temperature_c * (9 / 5) + 32
                        humidity = dhtDevice.humidity
                        self.updateHumidity(humidity)
                        self.updateTemperature(temperature_c)
                        
                        print("Temp: {}*C    Humidity: {}% ".format(temperature_c, humidity))
                    else:
                        print("Failed to read temperature data from DHT sensor.")
                    time.sleep(2.0)
                except Exception as e:
                    print("Exception occurred during DHT reading:", e)
        except KeyboardInterrupt:
            print("Keyboard interrupt received, stopping Humidity thread")
        finally:
            dhtDevice.exit()
            print("Humidity thread exited")


class sensorCode:
    def __init__(self):
        pass

cloud = Database_mongo()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Keyboard interrupt received, stopping program")
    cloud.stop_threads = True
    cloud.getRelay_thread.join()
    cloud.capture_thread.join()
    cloud.moisture_thread.join()
    cloud.capture_and_upload_thread.join()
    cloud.humidity.join()
    cloud.morter.join()
    print("All threads stopped successfully")
    GPIO.cleanup()
    exit()
