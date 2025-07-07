-- Identify this as a test.  
-- Pass the model and column as the parameters
-- REMEMBER:  Write your test as a negative in relation to the requirement.
--            The tests are run so that ZERO ROWS returned is a SUCCESS
--            If ANY rows are returned, it's considered a FAILURE
{% test employee_min_salary_check(model, column_name) %}

    -- The requirement is that all salary values should be >= 10,000.
    -- So, we write the negative test to see if any salary is < 10,000
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 10000

{% endtest %}. -- Be sure to close the test