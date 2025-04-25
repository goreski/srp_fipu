-- MySQL dump 10.13  Distrib 8.0.23, for Win64 (x86_64)
--
-- Host: localhost    Database: pentaho
-- ------------------------------------------------------
-- Server version	8.0.23

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `product`
--

DROP TABLE IF EXISTS `product`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `product` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  `product_type_fk` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `product_type_fk` (`product_type_fk`),
  CONSTRAINT `product_ibfk_1` FOREIGN KEY (`product_type_fk`) REFERENCES `product_type` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=144 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `product`
--

LOCK TABLES `product` WRITE;
/*!40000 ALTER TABLE `product` DISABLE KEYS */;
INSERT INTO `product` VALUES (1,'Ranger Vision',1),(2,'Seeker 35',1),(3,'Seeker Extreme',1),(4,'Seeker 50',1),(5,'Opera Vision',1),(6,'Seeker Mini',1),(7,'Glacier Deluxe',2),(8,'Glacier GPS',2),(9,'Trail Master',2),(10,'Trail Scout',2),(11,'Astro Pilot',2),(12,'Sky Pilot',2),(13,'Glacier GPS Extreme',2),(14,'Glacier Basic',2),(15,'Auto Pilot',2),(16,'BugShield Spray',3),(17,'BugShield Extreme',3),(18,'BugShield Lotion Lite',3),(19,'BugShield Lotion',3),(20,'BugShield Natural',3),(21,'Sun Shelter 30',4),(22,'Sun Shelter Stick',4),(23,'Sun Blocker',4),(24,'Sun Shelter 15',4),(25,'Sun Shield',4),(26,'Aloe Relief',5),(27,'Insect Bite Relief',5),(28,'Deluxe Family Relief Kit',5),(29,'Compact Relief Kit',5),(30,'Calamine Relief',5),(31,'Hailstorm Steel Irons',6),(32,'Hailstorm Titanium Irons',6),(33,'Lady Hailstorm Steel Irons',6),(34,'Lady Hailstorm Titanium Irons',6),(35,'Hailstorm Titanium Woods Set',7),(36,'Hailstorm Steel Woods Set',7),(37,'Lady Hailstorm Titanium Woods Set',7),(38,'Lady Hailstorm Steel Woods Set',7),(39,'Course Pro Putter',8),(40,'Blue Steel Putter',8),(41,'Blue Steel Max Putter',8),(42,'Course Pro Golf Bag',9),(43,'Course Pro Gloves',9),(44,'Course Pro Golf and Tee Set',9),(45,'Course Pro Umbrella',9),(46,'TrailChef Cup',10),(47,'TrailChef Kitchen Kit',10),(48,'TrailChef Double Flame',10),(49,'TrailChef Water Bag',10),(50,'TrailChef Cook Set',10),(51,'TrailChef Deluxe Cook Set',10),(52,'TrailChef Utensils',10),(53,'TrailChef Canteen',10),(54,'TrailChef Kettle',10),(55,'TrailChef Single Flame',10),(56,'Canyon Mule Extreme Backpack',11),(57,'Canyon Mule Journey Backpack',11),(58,'Canyon Mule Cooler',11),(59,'Canyon Mule Carryall',11),(60,'Canyon Mule Climber Backpack',11),(61,'Canyon Mule Weekender Backpack',11),(62,'Husky Rope 60',12),(63,'Husky Rope 100',12),(64,'Husky Rope 200',12),(65,'Husky Rope 50',12),(66,'Firefly Climbing Lamp',13),(67,'Firefly Charger',13),(68,'Firefly Rechargeable Battery',13),(69,'Granite Chalk Bag',13),(70,'Granite Carabiner',13),(71,'Granite Pulley',13),(72,'Granite Belay',13),(73,'Mountain Man Deluxe',14),(74,'Mountain Man Combination',14),(75,'Infinity',14),(76,'Lux',14),(77,'TX',14),(78,'Legend',14),(79,'Kodiak',14),(80,'Venue',14),(81,'Sam',14),(82,'Mountain Man Analog',14),(83,'Mountain Man Extreme',14),(84,'Mountain Man Digital',14),(85,'Zodiak',14),(86,'Polar Sun',15),(87,'Inferno',15),(88,'Maximus',15),(89,'Trendi',15),(90,'Polar Ice',15),(91,'Polar Sports',15),(92,'Cat Eye',15),(93,'Dante',15),(94,'Zone',15),(95,'Capri',15),(96,'Bella',15),(97,'Fairway',15),(98,'Hawk Eye',15),(99,'Retro',15),(100,'Polar Wave',15),(101,'Polar Extreme',15),(102,'Double Edge',16),(103,'Edge Extreme',16),(104,'Bear Survival Edge',16),(105,'Single Edge',16),(106,'Bear Edge',16),(107,'Max Gizmo',16),(108,'Pocket Gizmo',16),(109,'Star Lite',17),(110,'Star Dome',17),(111,'Star Gazer 6',17),(112,'Star Gazer 3',17),(113,'Star Peg',17),(114,'Star Gazer 2',17),(115,'Hibernator Pad',18),(116,'Hibernator Pillow',18),(117,'Hibernator Extreme',18),(118,'Hibernator Lite',18),(119,'Hibernator Self - Inflating Mat',18),(120,'Hibernator',18),(121,'Hibernator Camp Cot',18),(122,'Firefly Lite',19),(123,'Firefly Mapreader',19),(124,'Firefly 2',19),(125,'Firefly 4',19),(126,'EverGlow Double',19),(127,'Firefly Extreme',19),(128,'EverGlow Single',19),(129,'EverGlow Butane',19),(130,'Firefly Multi-light',19),(131,'EverGlow Lamp',19),(132,'EverGlow Kerosene',19),(133,'Flicker Lantern',19),(134,'Granite Climbing Helmet',20),(135,'Husky Harness',20),(136,'Husky Harness Extreme',20),(137,'Granite Signal Mirror',20),(138,'Granite Ice',21),(139,'Granite Shovel',21),(140,'Granite Grip',21),(141,'Granite Extreme',21),(142,'Granite Axe',21),(143,'Granite Hammer',21);
/*!40000 ALTER TABLE `product` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-04-23  9:07:51
