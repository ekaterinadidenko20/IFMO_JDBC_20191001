package com.efimchick.ifmo.web.jdbc.service;

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

public class ServiceFactory {

    private Employee employeeRowMapper(ResultSet resultSet, int lev) throws SQLException {
        try {
            Department department = null;
            if (resultSet.getString("DEPARTMENT") != null)
                for (int i = 0; i < getAllDeps().size(); i++) {
                    if (BigInteger.valueOf(Long.parseLong(resultSet.getString("DEPARTMENT"))).equals(getAllDeps().get(i).getId())) {
                        department = getAllDeps().get(i);
                    }
                }
            BigInteger manId = null;
            if (resultSet.getString("MANAGER") != null) {
                manId = BigInteger.valueOf(Long.parseLong(resultSet.getString("MANAGER")));
            }
            if (lev != 0)
                manId = null;
            Employee manager = null;
            if (manId != null) {
                manager = getEmployeeById(manId, lev).get();
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
                    manager,
                    department
            );
            return e;
        } catch (Exception e) {
            return null;
        }
    }

    private Employee employeeRowMapperWithChain(ResultSet resultSet) throws SQLException {
        try {
            Department department = null;
            if (resultSet.getString("DEPARTMENT") != null)
                for (int i = 0; i < getAllDeps().size(); i++) {
                    if (BigInteger.valueOf(Long.parseLong(resultSet.getString("DEPARTMENT"))).equals(getAllDeps().get(i).getId())) {
                        department = getAllDeps().get(i);
                    }
                }
            BigInteger manId = null;
            if (resultSet.getString("MANAGER") != null) {
                manId = BigInteger.valueOf(Long.parseLong(resultSet.getString("MANAGER")));
            }
            Employee manager = null;
            if (manId != null) {
                manager = getEmployeeByIdWithChain(manId).get();
            }

            return new Employee(
                    BigInteger.valueOf(resultSet.getInt("ID")),
                    new FullName(
                            resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")
                    ),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    BigDecimal.valueOf(resultSet.getInt("SALARY")),
                    manager,
                    department
            );
        } catch (Exception e) {
            return null;
        }
    }

    private ResultSet getResultSet(String s) throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection connection = connectionSource.createConnection();
        return connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(s);
    }


    List<Employee> getPaging(List<Employee> list, Paging paging) {
        return list.subList(paging.itemPerPage * (paging.page - 1), Math.min(paging.itemPerPage * paging.page, list.size()));
    }

    private Department departmentRowMapper(ResultSet resultSet) {
        try {
            return new Department(
                    BigInteger.valueOf(resultSet.getInt("ID")),
                    resultSet.getString("NAME"),
                    resultSet.getString("LOCATION")
            );
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }


    public Optional<Employee> getEmployeeByIdWithChain(BigInteger Id) {
        try {
            ResultSet resultSet = getResultSet("select * from employee where id = " + Id);
            List<Employee> employees = new ArrayList<>();
            while (resultSet.next()) {
                employees.add(employeeRowMapperWithChain(resultSet));
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

    public Optional<Employee> getEmployeeById(BigInteger Id, int lev) {
        try {
            ResultSet resultSet = getResultSet("select * from employee where id = " + Id);
            List<Employee> employees = new ArrayList<>();
            while (resultSet.next()) {
                employees.add(employeeRowMapper(resultSet, ++lev));
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

    public List<Employee> getEmployeeByDepartment(Department department) {
        try {
            ResultSet resultSet = getResultSet("select * from employee where department = " + department.getId());
            List<Employee> employees = new ArrayList<>();
            while (resultSet.next()) {
                employees.add(employeeRowMapper(resultSet,0));
            }
            return employees;
        } catch (SQLException e) {
            return null;
        }
    }

    public List<Employee> getEmployeeByManager(Employee employee) {
        try {
            ResultSet resultSet = getResultSet("select * from employee where manager = " + employee.getId());
            List<Employee> employees = new ArrayList<>();
            while (resultSet.next()) {
                employees.add(employeeRowMapper(resultSet,0));
            }
            return employees;
        } catch (SQLException e) {
            return null;
        }
    }


    public List<Employee> getAllEmployeesWithChain() {
        try {
            ResultSet resultSet = getResultSet("select * from employee");
            List<Employee> employees = new ArrayList<>();
            while (resultSet.next()) {
                employees.add(employeeRowMapperWithChain(resultSet));
            }
            return employees;
        } catch (SQLException e) {
            return null;
        }
    }

    public List<Employee> getAllEmployees() {
        try {
            ResultSet resultSet = getResultSet("select * from employee");
            List<Employee> employees = new ArrayList<>();
            while (resultSet.next()) {
                employees.add(employeeRowMapper(resultSet,0));
            }
            return employees;
        } catch (SQLException e) {
            return null;
        }
    }

    public List<Department> getAllDeps() {
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


    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                List<Employee> cur = getAllEmployees();
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                List<Employee> cur = getAllEmployees();
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                List<Employee> cur = getAllEmployees();
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getSalary().compareTo(o2.getSalary());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                List<Employee> cur = getAllEmployees();
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        if (o1.getDepartment() != null && o2.getDepartment() != null) {
                            if (o1.getDepartment().getName() == o2.getDepartment().getName()) {
                                return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                            } else {
                                return o1.getDepartment().getName().compareTo(o2.getDepartment().getName());
                            }
                        } else {
                            if (o1.getDepartment() == null) {
                                return -1;
                            }
                            if (o2.getDepartment() == null) {
                                return 1;
                            }
                            return 0;
                        }
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                List<Employee> cur = getEmployeeByDepartment(department);
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                List<Employee> cur = getEmployeeByDepartment(department);
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getSalary().compareTo(o2.getSalary());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                List<Employee> cur = getEmployeeByDepartment(department);
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                List<Employee> cur = getEmployeeByManager(manager);
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                List<Employee> cur = getEmployeeByManager(manager);
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getPaging(cur, paging);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                List<Employee> cur = getEmployeeByManager(manager);
                cur.sort((o1, o2) -> o1.getSalary().compareTo(o2.getSalary()));
                return getPaging(cur, paging);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                List<Employee> cur = getAllEmployeesWithChain();
                for (Employee e : cur) {
                    if (e.getId().equals(employee.getId())) {
                        return e;
                    }
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                List<Employee> cur = getAllEmployees();
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o2.getSalary().compareTo(o1.getSalary());
                    }
                });
                List<Employee> qwe = new ArrayList<>();
                for (Employee e : cur) {
                    if (e.getDepartment() != null && e.getDepartment().getId().equals(department.getId())) {
                        qwe.add(e);
                    }
                }
                return qwe.get(salaryRank - 1);
            }
        };
    }

}