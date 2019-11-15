package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

public class DaoFactory {

    List<Employee> Eml;

    {
        try {
            Eml = getEmployees();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    List<Department> Deps;

    {
        try {
            Deps = getDepartments();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Employee employeeRowMapper(ResultSet resultSet) throws SQLException {
        try {
            BigInteger manId = resultSet.getString("MANAGER") == null ? BigInteger.ZERO :BigInteger.valueOf(Long.parseLong(resultSet.getString("MANAGER")));
            BigInteger depId = resultSet.getString("DEPARTMENT") == null ? BigInteger.ZERO :BigInteger.valueOf(Long.parseLong(resultSet.getString("DEPARTMENT")));
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
                    manId,
                    depId
            );
            return e;
        } catch (Exception e) {
            return null;
        }
    }

    private ResultSet getResultSet(String s) throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection connection = connectionSource.createConnection();
        return connection.createStatement().executeQuery(s);
    }

    private List<Employee> getEmployees() throws SQLException {
        ResultSet resultSet = getResultSet("SELECT * FROM EMPLOYEE");
        List<Employee> employees = new LinkedList<>();
        while (resultSet.next()) {
            Employee employee = employeeRowMapper(resultSet);
            employees.add(employee);
        }
        return employees;
    }

    private List<Department> getDepartments() throws SQLException {
        ResultSet resultSet = getResultSet("SELECT * FROM DEPARTMENT");
        List<Department> departments = new LinkedList<>();
        while (resultSet.next()) {
            Department department = new Department(
                    BigInteger.valueOf(resultSet.getInt("ID")),
                    resultSet.getString("NAME"),
                    resultSet.getString("LOCATION")
            );
            departments.add(department);
        }
        return departments;
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                List<Employee> res = new ArrayList<>();
                for (int i = 0; i < Eml.size(); i++) {
                    Employee e = Eml.get(i);
                    if (e.getDepartmentId() != null && e.getDepartmentId().equals(department.getId()))
                        res.add(e);
                }
                return res;
            }

            @Override
            public List<Employee> getByManager(Employee manager) {
                List<Employee> res = new ArrayList<>();
                for (int i = 0; i < Eml.size(); i++) {
                    Employee e = Eml.get(i);
                    if (e.getManagerId() != null && e.getManagerId().equals(manager.getId())) {
                        res.add(e);
                    }
                }
                return res;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                for (int i = 0; i < Eml.size(); i++) {
                    Employee e = Eml.get(i);
                    if (e.getId().equals(Id)) {
                        return Optional.of(e);
                    }
                }
                return Optional.empty();
            }

            @Override
            public List<Employee> getAll() {
                return Eml;
            }


            @Override
            public Employee save(Employee employee) {
                Eml.add(employee);
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                Eml.remove(employee);
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                for (int i = 0; i < Deps.size(); i++) {
                    Department d = Deps.get(i);
                    if (d.getId().equals(Id)) {
                        return Optional.of(d);
                    }
                }
                return Optional.empty();
            }

            @Override
            public List<Department> getAll() {
                return Deps;
            }

            @Override
            public Department save(Department department) {
                for (int i = 0; i < Deps.size(); i++) {
                    if (Deps.get(i).getId().equals(department.getId())){
                        Deps.remove(Deps.get(i));
                    }
                }
                Deps.add(department);
                return department;
            }

            @Override
            public void delete(Department department) {
                Deps.remove(department);
            }
        };
    }
}