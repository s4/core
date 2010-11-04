/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.util;

public enum MetricsName {
    // metrics event name
    S4_APP_METRICS("S4::S4AppMetrics"), S4_EVENT_METRICS("S4::S4EventMetrics"), S4_CORE_METRICS(
            "S4::S4CoreMetrics"),

    // metrics name
    low_level_listener_msg_in_ct("lll_in"), low_level_listener_msg_drop_ct(
            "lll_dr"), low_level_listener_qsz("lll_qsz"), low_level_listener_badmsg_ct(
            "lll_bad"), // exception can't be caught
    generic_listener_msg_in_ct("gl_in"), pecontainer_ev_dq_ct("pec_dq"), pecontainer_ev_nq_ct(
            "pec_nq"), pecontainer_msg_drop_ct("pec_dr"), pecontainer_qsz(
            "pec_qsz"), pecontainer_qsz_w("pec_qsz_w"), pecontainer_ev_process_ct(
            "pec_pr"), pecontainer_pe_ct("pec_pe"), pecontainer_ev_err_ct(
            "pec_err"), // exception can't be caught
    pecontainer_exec_elapse_time("pec_exec_t"), low_level_emitter_msg_out_ct(
            "lle_out"), low_level_emitter_out_err_ct("lle_err"), low_level_emitter_qsz(
            "lle_qsz"), s4_core_exit_ct("s4_ex_ct"), s4_core_free_mem("s4_fmem"), pe_join_ev_ct(
            "pe_j_ct"), pe_error_count("pe_err");

    private final String eventShortName;

    private MetricsName(String eventShortName) {
        this.eventShortName = eventShortName;
    }

    public String toString() {
        return eventShortName;
    }

    public static void main(String[] args) {
        System.out.println(generic_listener_msg_in_ct.toString());

    }
}
