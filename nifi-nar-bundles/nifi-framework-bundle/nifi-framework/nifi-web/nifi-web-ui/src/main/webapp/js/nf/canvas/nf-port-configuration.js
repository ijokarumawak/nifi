/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'd3',
                'nf.ErrorHandler',
                'nf.Common',
                'nf.Dialog',
                'nf.Storage',
                'nf.Client',
                'nf.CanvasUtils',
                'nf.ng.Bridge',
                'nf.Port'],
            function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfCanvasUtils, nfNgBridge, nfPort) {
                return (nf.PortConfiguration = factory($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfCanvasUtils, nfNgBridge, nfPort));
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.PortConfiguration =
            factory(require('jquery'),
                require('d3'),
                require('nf.ErrorHandler'),
                require('nf.Common'),
                require('nf.Dialog'),
                require('nf.Storage'),
                require('nf.Client'),
                require('nf.CanvasUtils'),
                require('nf.ng.Bridge'),
                require('nf.Port')));
    } else {
        nf.PortConfiguration = factory(root.$,
            root.d3,
            root.nf.ErrorHandler,
            root.nf.Common,
            root.nf.Dialog,
            root.nf.Storage,
            root.nf.Client,
            root.nf.CanvasUtils,
            root.nf.ng.Bridge,
            root.nf.Port);
    }
}(this, function ($, d3, nfErrorHandler, nfCommon, nfDialog, nfStorage, nfClient, nfCanvasUtils, nfNgBridge, nfPort) {
    'use strict';

    /**
     * Initializes the port dialog.
     */
    var initPortConfigurationDialog = function () {
        $('#port-configuration').modal({
            scrollableContentStyle: 'scrollable',
            headerText: 'Configure Port',
            buttons: [{
                buttonText: 'Apply',
                color: {
                    base: '#728E9B',
                    hover: '#004849',
                    text: '#ffffff'
                },
                handler: {
                    click: function () {
                        // get the port data to reference the uri
                        var portId = $('#port-id').text();
                        var portData = d3.select('#id-' + portId).datum();

                        // build the updated port
                        var port = {
                            'id': portId,
                            'name': $('#port-name').val(),
                            'comments': $('#port-comments').val()
                        };

                        // include the concurrent tasks if appropriate
                        if ($('#port-concurrent-task-container').is(':visible')) {
                            port['concurrentlySchedulableTaskCount'] = $('#port-concurrent-tasks').val();
                        }

                        // mark the port disabled if appropriate
                        if ($('#port-enabled').hasClass('checkbox-unchecked')) {
                            port['state'] = 'DISABLED';
                        } else if ($('#port-enabled').hasClass('checkbox-checked')) {
                            port['state'] = 'STOPPED';
                        }

                        // mark the port remotely accessible if appropriate
                        if ($('#port-allow-remote-access').hasClass('checkbox-unchecked')) {
                            port['allowRemoteAccess'] = false;
                        } else if ($('#port-allow-remote-access').hasClass('checkbox-checked')) {
                            port['allowRemoteAccess'] = true;
                        }

                        // build the port entity
                        var portEntity = {
                            'revision': nfClient.getRevision(portData),
                            'disconnectedNodeAcknowledged': nfStorage.isDisconnectionAcknowledged(),
                            'component': port
                        };

                        // update the selected component
                        $.ajax({
                            type: 'PUT',
                            data: JSON.stringify(portEntity),
                            url: portData.uri,
                            dataType: 'json',
                            contentType: 'application/json'
                        }).done(function (response) {
                            // refresh the port component
                            nfPort.set(response);

                            // inform Angular app values have changed
                            nfNgBridge.digest();

                            // close the details panel
                            $('#port-configuration').modal('hide');
                        }).fail(function (xhr, status, error) {
                            // handle bad request locally to keep the dialog open, allowing the user
                            // to make changes. if the request fails for another reason, the dialog
                            // should be closed so the issue can be addressed (stale flow for instance)
                            if (xhr.status === 400) {
                                var errors = xhr.responseText.split('\n');

                                var content;
                                if (errors.length === 1) {
                                    content = $('<span></span>').text(errors[0]);
                                } else {
                                    content = nfCommon.formatUnorderedList(errors);
                                }

                                nfDialog.showOkDialog({
                                    dialogContent: content,
                                    headerText: 'Port Configuration'
                                });
                            } else {
                                // close the details panel
                                $('#port-configuration').modal('hide');

                                // handle the error
                                nfErrorHandler.handleAjaxError(xhr, status, error);
                            }
                        });
                    }
                }
            },
                {
                    buttonText: 'Cancel',
                    color: {
                        base: '#E3E8EB',
                        hover: '#C7D2D7',
                        text: '#004849'
                    },
                    handler: {
                        click: function () {
                            $('#port-configuration').modal('hide');
                        }
                    }
                }],
            handler: {
                close: function () {
                    // clear the port details
                    $('#port-id').text('');
                    $('#port-name').val('');
                    $('#port-enabled').removeClass('checkbox-unchecked checkbox-checked');
                    $('#port-concurrent-tasks').val('');
                    $('#port-comments').val('');
                    $('#port-allow-remote-access').removeClass('checkbox-unchecked checkbox-checked');
                }
            }
        });
    };

    return {
        init: function () {
            initPortConfigurationDialog();
        },

        /**
         * Shows the details for the specified selection.
         *
         * @argument {selection} selection      The selection
         */
        showConfiguration: function (selection) {
            // if the specified component is a port, load its properties
            if (nfCanvasUtils.isInputPort(selection) || nfCanvasUtils.isOutputPort(selection)) {
                var selectionData = selection.datum();

                // determine if the enabled checkbox is checked or not
                var portEnableStyle = 'checkbox-checked';
                if (selectionData.component.state === 'DISABLED') {
                    portEnableStyle = 'checkbox-unchecked';
                }

                // show concurrent tasks for root groups only
                if (selectionData.allowRemoteAccess === true) {
                    $('#port-concurrent-task-container').show();
                } else {
                    $('#port-concurrent-task-container').hide();
                }

                // populate the port settings
                $('#port-id').text(selectionData.id);
                $('#port-name').val(selectionData.component.name);
                $('#port-enabled').removeClass('checkbox-unchecked checkbox-checked').addClass(portEnableStyle);
                $('#port-concurrent-tasks').val(selectionData.component.concurrentlySchedulableTaskCount);
                $('#port-comments').val(selectionData.component.comments);
                $('#port-allow-remote-access').removeClass('checkbox-unchecked checkbox-checked')
                    .addClass(true === selectionData.component.allowRemoteAccess ? 'checkbox-checked' : 'checkbox-unchecked');

                // show the details
                $('#port-configuration').modal('show');
            }
        }
    };
}));