/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Created by master on 17-11-27.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.ArrayList;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

class LogsPage extends RmView {

    static class LogsBlock extends HtmlBlock {
        final ResourceManager rm;

        @Inject
        LogsBlock(ResourceManager rm, ViewContext ctx) {
            super(ctx);
            this.rm = rm;
        }

        @Override
        protected void render(Block html) {
            html._(MetricsOverviewTable.class);
            TBODY<TABLE<Hamlet>> tbody = html.table("#dda").
                    thead().
                    tr().
                    th(".timestamp", "TimeStamp").
                    th(".logs", "Logs").
                    _()._().
                    tbody();

            ArrayList<String> logs = this.rm.getRMContext().getLogsService().getLogs();
            for(String str : logs) {
                String timeStr = str.split(" ")[0];
                TR<TBODY<TABLE<Hamlet>>> row = tbody.tr().
                        td().br().$title(timeStr)._().
                        _(Times.format(Long.parseLong(timeStr)))._();
                row.td(str.replace(timeStr + " ", ""))._();
            }
            tbody._()._();
        }
    }

    @Override protected void preHead(Page.HTML<_> html) {
        commonPreHead(html);
        String title = "Resource Allocation Logs of the Cluster";
        setTitle(title);
        set(DATATABLES_ID, "dda");
        set(initID(DATATABLES, "dda"), nodesTableInit());
        setTableStyles(html, "dda", ".logs {width:70em}");
    }

    @Override protected Class<? extends SubView> content() {
        return LogsBlock.class;
    }

    private String nodesTableInit() {
        StringBuilder a = tableInit().append(", aoColumnDefs: [" +
                "{'bSearchable': false, 'aTargets': [ 1 ]}" +
                ", {'sType': 'title-numeric', 'aTargets': [ 0 ]}" + "]}");
        return a.toString();
    }
}
