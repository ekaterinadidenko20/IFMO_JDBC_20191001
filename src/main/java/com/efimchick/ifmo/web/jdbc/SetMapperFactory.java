package com.efimchick.ifmo.web.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    private Employee getEmployeeById(ResultSet resultSet, Integer id) throws SQLException {
        Employee e = null;
        if (id == null)
            return null;
        int begin = resultSet.getRow();
        resultSet.beforeFirst();
        while (resultSet.next()) {
            if (resultSet.getInt("ID") == id) {
                e = employeeRowMapper(resultSet);
                break;
            }
        }
        resultSet.absolute(begin);
        return e;
    }

    private Employee employeeRowMapper(ResultSet resultSet) throws SQLException {
        try {
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
                    getEmployeeById(resultSet, resultSet.getInt("MANAGER"))
            );
            return e;
        }
        catch (Exception e){
            return null;
        }
    }

    public SetMapper<Set<Employee>> employeesSetMapper() {
        SetMapper<Set<Employee>> sm = new SetMapper<Set<Employee>>() {
            @Override
            public Set<Employee> mapSet(ResultSet resultSet) {
                Set<Employee> set = new HashSet<>();
                try {
                    while (resultSet.next()) {
                        set.add(employeeRowMapper(resultSet));
                    }
                } catch (Exception e) {
                }
                return set;
            }
        };
        return sm;
    }

}