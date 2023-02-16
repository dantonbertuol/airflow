CREATE TABLE `sales` (
    `id` int(8) NOT NULL AUTO_INCREMENT,
    `data` varchar(255),
    `valor` float default NULL,
    PRIMARY KEY (`id`)
) AUTO_INCREMENT=1;

INSERT INTO `sales` (`data`, `valor`) VALUES ('2020-01-01', 100);
INSERT INTO `sales` (`data`, `valor`) VALUES ('2020-01-02', 200);
INSERT INTO `sales` (`data`, `valor`) VALUES ('2020-01-03', 300);
INSERT INTO `sales` (`data`, `valor`) VALUES ('2020-01-04', 400);
INSERT INTO `sales` (`data`, `valor`) VALUES ('2020-01-05', 500);
INSERT INTO `sales` (`data`, `valor`) VALUES ('2020-01-06', 600);

DROP TABLE IF EXISTS sales_temp;

CREATE TABLE sales_temp AS
SELECT * FROM sales
WHERE data BETWEEN '2020-01-01' AND '2020-01-05';