package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

public class DaoFactory {

    private Employee employeeRowMapper(ResultSet resultSet) {
        try {
            BigInteger managerId = BigInteger.ZERO;
            BigInteger departmentId = BigInteger.ZERO;
            if (!Position.valueOf(resultSet.getString("POSITION")).equals(Position.PRESIDENT)) {
                managerId = BigInteger.valueOf(resultSet.getInt("MANAGER"));
                departmentId = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));
            }
            Employee e = new Employee(
                    BigInteger.valueOf(resultSet.getInt("ID")),
                    new FullName(
                            resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")
                    ),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    BigDecimal.valueOf(resultSet.getInt("SALARY")),
                    managerId,
                    departmentId
            );
            return e;
        } catch (Exception e) {
            return null;
        }
    }

    private Department departmentRowMapper(ResultSet resultSet) {
        try {
            Department department = new Department(
                    BigInteger.valueOf(resultSet.getInt("ID")),
                    resultSet.getString("NAME"),
                    resultSet.getString("LOCATION")
            );
            return department;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private ResultSet getResultSet(String s) throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection connection = connectionSource.createConnection();
        return connection.createStatement().executeQuery(s);
    }

    private PreparedStatement getPreparedStatement(String s) throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection connection = connectionSource.createConnection();
        return connection.prepareStatement(s);
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                try {
                    ResultSet resultSet = getResultSet("select * from employee where department = " + department.getId());
                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(employeeRowMapper(resultSet));
                    }
                    return employees;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                try {
                    ResultSet resultSet = getResultSet("select * from employee where manager = " + employee.getId());
                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(employeeRowMapper(resultSet));
                    }
                    return employees;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try {
                    ResultSet resultSet = getResultSet("select * from employee where id = " + Id);
                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(employeeRowMapper(resultSet));
                    }
                    if (employees.isEmpty()) {
                        return Optional.empty();
                    } else {
                        return Optional.of(employees.get(0));
                    }
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                try {
                    ResultSet resultSet = getResultSet("select * from employee");
                    List<Employee> employees = new ArrayList<>();
                    while (resultSet.next()) {
                        employees.add(employeeRowMapper(resultSet));
                    }
                    return employees;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Employee save(Employee employee) {
                try {
                    PreparedStatement preparedStatement = getPreparedStatement("insert into employee values (?,?,?,?,?,?,?,?,?)");
                    preparedStatement.setInt(1, employee.getId().intValue());
                    preparedStatement.setString(2, employee.getFullName().getFirstName());
                    preparedStatement.setString(3, employee.getFullName().getLastName());
                    preparedStatement.setString(4, employee.getFullName().getMiddleName());
                    preparedStatement.setString(5, employee.getPosition().toString());
                    preparedStatement.setInt(6, employee.getManagerId().intValue());
                    preparedStatement.setString(7, employee.getHired().toString());
                    preparedStatement.setDouble(8, employee.getSalary().doubleValue());
                    preparedStatement.setInt(9, employee.getDepartmentId().intValue());
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                try {
                    PreparedStatement preparedStatement = getPreparedStatement("delete from employee where id = " + employee.getId());
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                try {
                    ResultSet resultSet = getResultSet("select * from department where id = " + Id);
                    List<Department> departments = new ArrayList<>();
                    while (resultSet.next()) {
                        departments.add(departmentRowMapper(resultSet));
                    }
                    if (departments.isEmpty()) {
                        return Optional.empty();
                    } else {
                        return Optional.of(departments.get(0));
                    }
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                try {
                    ResultSet resultSet = getResultSet("select * from department ");
                    List<Department> departments = new ArrayList<>();
                    while (resultSet.next()) {
                        departments.add(departmentRowMapper(resultSet));
                    }
                    return departments;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                try {
                    Optional<Department> optionalDepartment = getById(department.getId());
                    if (optionalDepartment.isPresent()){
                        PreparedStatement preparedStatement = getPreparedStatement(
                                "update department set name=?,location=? where id=?");
                        preparedStatement.setInt(3, department.getId().intValue());
                        preparedStatement.setString(1, department.getName());
                        preparedStatement.setString(2, department.getLocation());
                        preparedStatement.executeUpdate();
                    } else {
                        PreparedStatement preparedStatement = getPreparedStatement("insert into department values (?,?,?)");
                        preparedStatement.setInt(1, department.getId().intValue());
                        preparedStatement.setString(2, department.getName());
                        preparedStatement.setString(3, department.getLocation());
                        preparedStatement.executeUpdate();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return department;
            }

            @Override
            public void delete(Department department) {
                try {
                    PreparedStatement preparedStatement = getPreparedStatement("delete from department where id = " + department.getId());
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}