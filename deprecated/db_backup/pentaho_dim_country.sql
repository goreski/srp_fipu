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
-- Table structure for table `dim_country`
--

DROP TABLE IF EXISTS `dim_country`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_country` (
  `country_tk` bigint NOT NULL,
  `version` int DEFAULT NULL,
  `date_from` datetime DEFAULT NULL,
  `date_to` datetime DEFAULT NULL,
  `country_id` int DEFAULT NULL,
  `country` varchar(45) DEFAULT NULL,
  `region` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`country_tk`),
  KEY `idx_dim_country_lookup` (`country_id`),
  KEY `idx_dim_country_tk` (`country_tk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dim_country`
--

LOCK TABLES `dim_country` WRITE;
/*!40000 ALTER TABLE `dim_country` DISABLE KEYS */;
INSERT INTO `dim_country` VALUES (0,1,NULL,NULL,NULL,NULL,NULL),(1,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',8,'Australia','Oceania'),(2,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',18,'Austria','Europe'),(3,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',17,'Belgium','Europe'),(4,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',3,'Brazil','Americas'),(5,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',1,'Canada','Americas'),(6,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',7,'China','Asia'),(7,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',11,'Denmark','Europe'),(8,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',12,'Finland','Europe'),(9,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',13,'France','Europe'),(10,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',14,'Germany','Europe'),(11,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',19,'Italy','Europe'),(12,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',4,'Japan','Asia'),(13,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',2,'Mexico','Americas'),(14,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',9,'Netherlands','Europe'),(15,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',6,'Poland','Europe'),(16,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',5,'Singapore','Asia'),(17,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',20,'Spain','Europe'),(18,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',10,'Sweden','Europe'),(19,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',15,'Switzerland','Europe'),(20,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',16,'United Kingdom','Europe'),(21,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',21,'United States','Americas');
/*!40000 ALTER TABLE `dim_country` ENABLE KEYS */;
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
