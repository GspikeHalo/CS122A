CREATE DATABASE IF NOT EXISTS cs122a;
USE cs122a;

CREATE USER 'test'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON cs122a.* TO 'test'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;


CREATE TABLE Users (
  uid INTEGER,
  nickname VARCHAR(20),
  email VARCHAR(125),
  street VARCHAR(50),
  city VARCHAR(50),
  state VARCHAR(50),
  zip VARCHAR(50),
  genres TEXT NOT NULL,
  joined_date DATE,
  PRIMARY KEY (uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Producers (
  uid INTEGER,
  company VARCHAR(125),
  bio VARCHAR(255),
  PRIMARY KEY (uid),
  FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Viewers (
  uid INTEGER,
  first_name VARCHAR(20),
  last_name VARCHAR(20),
  subscription ENUM('free', 'monthly', 'yearly'),
  PRIMARY KEY (uid),
  FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Releases (
  rid INTEGER,
  producer_uid INTEGER NOT NULL,
  title VARCHAR(20),
  genre VARCHAR(20),
  release_date DATE,
  PRIMARY KEY (rid),
  FOREIGN KEY (producer_uid) REFERENCES Producers(uid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Movies (
  rid INTEGER,
  website_url VARCHAR(255),
  PRIMARY KEY (rid),
  FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Series (
  rid INTEGER,
  introduction TEXT,
  PRIMARY KEY (rid),
  FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Videos (
  ep_num INTEGER NOT NULL,
  rid INTEGER NOT NULL,
  title VARCHAR(20),
  length REAL,
  PRIMARY KEY (ep_num, rid),
  FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Sessions (
  sid INTEGER,
  quality ENUM('480p', '720p', '1080p'),
  device ENUM('mobile', 'desktop'),
  ep_num INTEGER NOT NULL,
  rid INTEGER NOT NULL,
  uid INTEGER NOT NULL,
  initiate_at DATETIME,
  leave_at DATETIME,
  PRIMARY KEY (sid),
  FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
  FOREIGN KEY (ep_num, rid) REFERENCES Videos(ep_num, rid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE Reviews (
  rvid INTEGER,
  rating INTEGER,
  body VARCHAR(125),
  uid INTEGER NOT NULL,
  posted_at DATETIME,
  rid INTEGER NOT NULL,
  PRIMARY KEY (rvid),
  FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
  FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
