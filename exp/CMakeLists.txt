#Testing macro
macro (add_exp_executable_with_lib _name libs)
	add_executable(exp_${_name} "${_name}.cpp")
	target_link_libraries(exp_${_name} ${EMP-OT_LIBRARIES})
endmacro()

macro (add_exp_case _name)
	add_exp_executable_with_lib(${_name} "")
  	add_test(NAME ${_name} COMMAND "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_${_name}" WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/")
endmacro()

macro (add_exp_case_with_run _name)
	add_exp_executable_with_lib(${_name} "")
	add_test(NAME ${_name} COMMAND "./run" "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_${_name}" WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/")

endmacro()


IF(${ENABLE_FLOAT})
ENDIF(${ENABLE_FLOAT})

#Experiments
add_exp_case_with_run(q1)
add_exp_case_with_run(q1_a)
add_exp_case_with_run(q1_b)


add_exp_case_with_run(q2)
add_exp_case_with_run(q2_a)
add_exp_case_with_run(q2_b)
add_exp_case_with_run(q2_eps)


add_exp_case_with_run(q3)
add_exp_case_with_run(q3_a)
add_exp_case_with_run(q3_b)


add_exp_case_with_run(q4)
add_exp_case_with_run(q4_a)
add_exp_case_with_run(q4_b)
add_exp_case_with_run(q4_eps)


add_exp_case_with_run(q5)
add_exp_case_with_run(q5_b)
add_exp_case_with_run(q5_a)


add_exp_case_with_run(q6)
add_exp_case_with_run(q6_b)


add_exp_case_with_run(q7)
add_exp_case_with_run(q7_b)


add_exp_case_with_run(q8)
add_exp_case_with_run(q8_b)
add_exp_case_with_run(q8_eps)


add_exp_case_with_run(q2_scale)
add_exp_case_with_run(q4_scale)
add_exp_case_with_run(q8_scale)


add_exp_case_with_run(selection)
add_exp_case_with_run(parallel_join)
add_exp_case_with_run(plan_test)









