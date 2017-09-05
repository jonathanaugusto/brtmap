CREATE TABLE `gpsdata` (
  `codigo` varchar(30) DEFAULT NULL,
  `linha` varchar(15) DEFAULT NULL,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `datahora` timestamp NULL DEFAULT NULL,
  `velocidade` double DEFAULT NULL,
  `sentido` varchar(15) DEFAULT NULL,
  `trajeto` varchar(100) DEFAULT NULL,
  `corredor` varchar(20) DEFAULT NULL
);

-- CREATE TABLE `itinerario` (
--   `linha` int(11) NOT NULL,
--   `sentido` varchar(15) DEFAULT NULL,
--   `trajeto` varchar(100) DEFAULT NULL,
--   `destino` varchar(50) DEFAULT NULL,
--   `sequencia` int(11) DEFAULT NULL,
--   `estacao` varchar(100) DEFAULT NULL
-- );


-- CREATE TABLE `stats` (
--   `window_start` timestamp NULL DEFAULT NULL,
--   `window_end` timestamp NULL DEFAULT NULL,
--   `corredor` varchar(20) DEFAULT NULL,
--  `linha` varchar(15) DEFAULT NULL,
--  `sentido` varchar(15) DEFAULT NULL,
--   `vel_media` double DEFAULT NULL,
--   `qtd_carros` int DEFAULT NULL,
--   `datahora` timestamp NULL DEFAULT NULL
-- );


CREATE TABLE `stats_vel` (

  `corredor` varchar(20) DEFAULT NULL,
--  `linha` varchar(15) DEFAULT NULL,
--  `sentido` varchar(15) DEFAULT NULL,
  `vel_media` double DEFAULT NULL,
  `data` date NULL DEFAULT NULL,
  `hora` time NULL DEFAULT NULL,
  `atualizacao` timestamp NULL DEFAULT NULL
);



CREATE TABLE `stats_qtd` (

  `corredor` varchar(20) DEFAULT NULL,
--  `linha` varchar(15) DEFAULT NULL,
--  `sentido` varchar(15) DEFAULT NULL,
  `qtd_carros` int DEFAULT NULL,
  `data` date NULL DEFAULT NULL,
  `hora` time NULL DEFAULT NULL,
  `atualizacao` timestamp NULL DEFAULT NULL
);


