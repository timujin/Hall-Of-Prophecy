CREATE TABLE `Users` (
	`id` bigint NOT NULL AUTO_INCREMENT,
	`oauthToken` mediumtext(20) UNIQUE,
	`name` longtext(40),
	`avatarID` mediumtext(20) UNIQUE,
	`graphDataID` mediumtext(20) UNIQUE,
	`isGraphStale` bool,
	PRIMARY KEY (`id`)
);

CREATE TABLE `Predictions` (
	`id` bigint NOT NULL AUTO_INCREMENT,
	`name` longtext(100) NOT NULL,
	`description` longtext(1000),
	`authorID` bigint NOT NULL,
	`url` longtext(40) NOT NULL,
	`public` char NOT NULL DEFAULT 'p',
	`result` bool DEFAULT 'null',
	PRIMARY KEY (`id`)
);

CREATE TABLE `Wagers` (
	`id` bigint NOT NULL AUTO_INCREMENT,
	`authorID` bigint NOT NULL,
	`predictionID` bigint NOT NULL,
	`value` double NOT NULL DEFAULT '1',
	PRIMARY KEY (`id`)
);

CREATE TABLE `Comments` (
	`id` bigint NOT NULL AUTO_INCREMENT,
	`text` longtext(1000),
	`authorID` bigint NOT NULL,
	`preditctionID` bigint NOT NULL,
	PRIMARY KEY (`id`)
);

ALTER TABLE `Predictions` ADD CONSTRAINT `Predictions_fk0` FOREIGN KEY (`authorID`) REFERENCES `Users`(`id`);

ALTER TABLE `Wagers` ADD CONSTRAINT `Wagers_fk0` FOREIGN KEY (`authorID`) REFERENCES `Users`(`id`);

ALTER TABLE `Wagers` ADD CONSTRAINT `Wagers_fk1` FOREIGN KEY (`predictionID`) REFERENCES `Predictions`(`id`);

ALTER TABLE `Comments` ADD CONSTRAINT `Comments_fk0` FOREIGN KEY (`authorID`) REFERENCES `Users`(`id`);

ALTER TABLE `Comments` ADD CONSTRAINT `Comments_fk1` FOREIGN KEY (`preditctionID`) REFERENCES `Predictions`(`id`);

