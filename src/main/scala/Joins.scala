INNER JOIN
==========

SELECT Products.CategoryID, ProductName, Price, CategoryName, Description
FROM
Products INNER JOIN Categories ON
Products.CategoryID=Categories.CategoryID;

LEFT OUTER JOIN
==========

SELECT Customers.CustomerName, Orders.EmployeeID
FROM Customers
  LEFT OUTER JOIN Orders
  ON Customers.CustomerID = Orders.CustomerID;

RIGHT OUTER JOIN
===========

SELECT Orders.OrderID, Orders.OrderDate, Employees.LastName, Employees.FirstName
FROM Orders
  RIGHT OUTER JOIN Employees
  ON Orders.EmployeeID = Employees.EmployeeID;

FULL JOIN
=========

SELECT Customers.CustomerName, Orders.OrderID
FROM Customers
  FULL OUTER JOIN Orders ON Customers.CustomerID=Orders.CustomerID;

SELF JOIN
==========



