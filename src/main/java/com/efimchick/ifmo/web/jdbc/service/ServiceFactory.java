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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;


public class ServiceFactory {

    List<Employee> Eml;
    List<Employee> EmlForChain;

    {
        try {
            Eml = getEmployees();
            EmlForChain = getEmployees();
            fillManagers1lev();
            fillManagersInfLev();
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
            Department department = null;
            if (resultSet.getString("DEPARTMENT") != null)
                for (int i = 0; i < getDepartments().size(); i++) {
                    if (BigInteger.valueOf(Long.parseLong(resultSet.getString("DEPARTMENT"))).equals(getDepartments().get(i).getId())) {
                        department = getDepartments().get(i);
                    }
                }
            BigInteger manId = null;
            if (resultSet.getString("MANAGER") != null) {
                manId = BigInteger.valueOf(Long.parseLong(resultSet.getString("MANAGER")));
            }
            Employee manager = null;
            if (manId != null) {
                manager = new Employee(
                        manId,
                        null,
                        null,
                        null,
                        BigDecimal.ZERO,
                        null,
                        null
                );
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

    void fillManagers1lev() {
        for (int i = 0; i < Eml.size(); ++i) {
            if (Eml.get(i).getManager() != null) {
                for (int j = 0; j < Eml.size(); ++j) {
                    if (Eml.get(j).getId().equals(Eml.get(i).getManager().getId())) {
                        Employee cur = Eml.get(i);
                        Employee cur2 = Eml.get(j);
                        Eml.set(i, new Employee(
                                cur.getId(),
                                cur.getFullName(),
                                cur.getPosition(),
                                cur.getHired(),
                                cur.getSalary(),
                                new Employee(
                                        cur2.getId(),
                                        cur2.getFullName(),
                                        cur2.getPosition(),
                                        cur2.getHired(),
                                        cur2.getSalary(),
                                        null,
                                        cur2.getDepartment()
                                ),
                                cur.getDepartment()
                        ));
                    }
                }
            }
        }
    }

    void fillManagersInfLev() {
        for (int k = 0; k < 5; ++k)
            for (int i = 0; i < EmlForChain.size(); ++i) {
                if (EmlForChain.get(i).getManager() != null) {
                    for (int j = 0; j < EmlForChain.size(); ++j) {
                        if (EmlForChain.get(j).getId().equals(EmlForChain.get(i).getManager().getId())) {
                            Employee cur = EmlForChain.get(i);
                            Employee cur2 = EmlForChain.get(j);
                            EmlForChain.set(i, new Employee(
                                    cur.getId(),
                                    cur.getFullName(),
                                    cur.getPosition(),
                                    cur.getHired(),
                                    cur.getSalary(),
                                    cur2,
                                    cur.getDepartment()
                            ));
                        }
                    }
                }
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

    List getPaging(List list, Paging paging) {
        return list.subList(paging.itemPerPage * (paging.page - 1), Math.min(paging.itemPerPage * paging.page, list.size()));
    }

    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                List<Employee> cur = Eml;
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
                List<Employee> cur = Eml;
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
                List<Employee> cur = Eml;
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
                List<Employee> cur = Eml;
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
                List<Employee> cur = Eml;
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getEmployees2(department, paging, cur);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                List<Employee> cur = Eml;
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getSalary().compareTo(o2.getSalary());
                    }
                });
                return getEmployees2(department, paging, cur);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                List<Employee> cur = Eml;
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getEmployees2(department, paging, cur);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                List<Employee> cur = Eml;
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return getEmployees(manager, paging, cur);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                List<Employee> cur = Eml;
                cur.sort(new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getHired().compareTo(o2.getHired());
                    }
                });
                return getEmployees(manager, paging, cur);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                List<Employee> cur = Eml;
                cur.sort((o1, o2) -> o1.getSalary().compareTo(o2.getSalary()));
                return getEmployees(manager, paging, cur);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                List<Employee> cur = EmlForChain;
                for (Employee e : cur){
                    if (e.getId().equals(employee.getId())){
                        return e;
                    }
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                List<Employee> cur = Eml;
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

    private List<Employee> getEmployees2(Department department, Paging paging, List<Employee> cur) {
        List<Employee> qwe = new ArrayList<>();
        for (Employee e : cur) {
            if (e.getDepartment() != null && e.getDepartment().getId().equals(department.getId())) {
                qwe.add(e);
            }
        }
        return getPaging(qwe, paging);
    }

    private List<Employee> getEmployees(Employee manager, Paging paging, List<Employee> cur) {
        List<Employee> qwe = new ArrayList<>();
        for (Employee e : cur) {
            if (e.getManager() != null && e.getManager().getId().equals(manager.getId())) {
                qwe.add(e);
            }
        }
        return getPaging(qwe, paging);
    }

}