﻿import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import pointChange = require("models/timeSeries/pointChange");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");

class editPointDialog extends dialogViewModelBase {

    public updateTask = $.Deferred();
    updateTaskStarted = false;
    editedPoint = ko.observable<pointChange>();
    isNew: KnockoutComputed<boolean>;

    constructor(editedPoint?: pointChange) {
        super();
        this.editedPoint(!editedPoint ? new pointChange(new timeSeriesPoint("", ["Field 1", "Todo 2"], "", "", [0, 0]), true) : editedPoint);
        this.isNew = ko.computed(() => !!this.editedPoint() && this.editedPoint().isNew());
    }

    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        this.updateTaskStarted = true;
        this.updateTask.resolve(this.editedPoint());
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.updateTaskStarted) {
            this.updateTask.reject();
        }
    }
}

export = editPointDialog;