package com.efimchick.ifmo.web.jdbc;

/**
 * Implement sql queries like described
 */
public class SqlQueries {
    //Select all employees sorted by last name in ascending order
    //language=HSQLDB
    String select01 = "SELECT * from EMPLOYEE ORDER BY LASTNAME";

    //Select employees having no more than 5 characters in last name sorted by last name in ascending order
    //language=HSQLDB
    String select02 = "SELECT * from EMPLOYEE where length(LASTNAME) < 6 order by LASTNAME";

    //Select employees having salary no less than 2000 and no more than 3000
    //language=HSQLDB
    String select03 = "SELECT * from EMPLOYEE where SALARY between 2000 and 3000";

    //Select employees having salary no more than 2000 or no less than 3000
    //language=HSQLDB
    String select04 = "SELECT * from EMPLOYEE where SALARY <= 2000 OR SALARY >= 3000";

    //Select employees assigned to a department and corresponding department name
    //language=HSQLDB
    String select05 = "SELECT LASTNAME,NAME,SALARY from EMPLOYEE INNER JOIN DEPARTMENT on EMPLOYEE.DEPARTMENT = DEPARTMENT.ID";

    //Select all employees and corresponding department name if there is one.
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select06 = "SELECT LASTNAME,NAME depname,SALARY from EMPLOYEE LEFT JOIN DEPARTMENT on EMPLOYEE.DEPARTMENT = DEPARTMENT.ID";

    //Select total salary pf all employees. Name it "total".
    //language=HSQLDB
    String select07 = "SELECT sum(SALARY) as total from EMPLOYEE";

    //Select all departments and amount of employees assigned per department
    //Name column containing name of the department "depname".
    //Name column containing employee amount "staff_size".
    //language=HSQLDB
    String select08 = "SELECT DEPARTMENT.NAME as depname,count(DEPARTMENT.NAME) as staff_size FROM EMPLOYEE INNER JOIN DEPARTMENT on EMPLOYEE.DEPARTMENT = DEPARTMENT.ID group by DEPARTMENT.NAME";

    //Select all departments and values of total and average salary per department
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select09 = "SELECT DEPARTMENT.NAME as depname,sum(EMPLOYEE.SALARY) as total, avg(EMPLOYEE.SALARY) as average  FROM EMPLOYEE INNER JOIN DEPARTMENT on EMPLOYEE.DEPARTMENT = DEPARTMENT.ID group by DEPARTMENT.NAME";

    //Select all employees and their managers if there is one.
    //Name column containing employee lastname "employee".
    //Name column containing manager lastname "manager".
    //language=HSQLDB
    String select10 = "SELECT emp.LASTNAME as employee, man.LASTNAME as manager FROM EMPLOYEE as emp LEFT JOIN EMPLOYEE as man on emp.MANAGER = man.ID";


}