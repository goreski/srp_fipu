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
-- Table structure for table `dim_retailer`
--

DROP TABLE IF EXISTS `dim_retailer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_retailer` (
  `retailer_tk` bigint NOT NULL,
  `version` int DEFAULT NULL,
  `date_from` datetime DEFAULT NULL,
  `date_to` datetime DEFAULT NULL,
  `retailer_id` int DEFAULT NULL,
  `name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`retailer_tk`),
  KEY `idx_dim_retailer_lookup` (`retailer_id`),
  KEY `idx_dim_retailer_tk` (`retailer_tk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dim_retailer`
--

LOCK TABLES `dim_retailer` WRITE;
/*!40000 ALTER TABLE `dim_retailer` DISABLE KEYS */;
INSERT INTO `dim_retailer` VALUES (0,1,NULL,NULL,NULL,NULL),(1,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',4,'Department Store'),(2,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',7,'Direct Marketing'),(3,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',8,'Equipment Rental Store'),(4,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',6,'Eyewear Store'),(5,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',3,'Golf Shop'),(6,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',2,'Outdoors Shop'),(7,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',1,'Sports Store'),(8,1,'1900-01-01 00:00:00','2200-01-01 00:00:00',5,'Warehouse Store');
/*!40000 ALTER TABLE `dim_retailer` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-04-23  9:07:52
