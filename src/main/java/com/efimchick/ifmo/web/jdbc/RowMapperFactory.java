package com.efimchick.ifmo.web.jdbc;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.time.LocalDate;

public class RowMapperFactory {

    public RowMapper<Employee> employeeRowMapper() {
        RowMapper<Employee> rm = new RowMapper<Employee>() {
            @Override
            public Employee mapRow(ResultSet resultSet) {
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
                            BigDecimal.valueOf(resultSet.getInt("SALARY"))
                    );
                    return e;
                }
                catch (Exception e){
                    return null;
                }
            }
        };
        return rm;
    }
}