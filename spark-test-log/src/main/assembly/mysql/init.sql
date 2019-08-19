CREATE TABLE `log_count` (
  `log` varchar(255) COLLATE utf8_bin NOT NULL,
  `value` int(11) DEFAULT NULL,
  PRIMARY KEY (`log`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
