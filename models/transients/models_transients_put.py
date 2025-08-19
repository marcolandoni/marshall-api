#!/usr/local/bin/python
# encoding: utf-8
"""
:Author:
    David Young
:Modified by:
    Marco Landoni for Flask porting
"""
from builtins import zip
from builtins import object
from fundamentals.mysql import readquery, writequery
import sys
import os

from datetime import datetime, date, time


class models_transients_element_put(object):
    """
    The worker class for the models_transients_element_put module

    **Key Arguments**

    - ``log`` -- logger
    - ``request`` -- the pyramid request
    """

    def __init__(
        self,
        log,
        request,
        db
    ):
        self.log = log
        self.request = request
        self.transientBucketId = request["elementId"]
        self.response = ""
        self.dbConn = db
        # xt-self-arg-tmpx

        log.debug("instansiating a new 'models_transients_element_put' object")

        return None

    def close(self):
        del self
        return None

    def put(self):
        """get the models_transients_element_put object

        **Return**

        - ``response``
        """
        self.log.debug('starting the ``get`` method')

        # move the objects to another list if requested
        if "mwl" in self.request or "awl" in self.request:
            self._move_transient_to_another_list()
            return self.response

        # change the pi is requested
        if set(("piName", "piEmail")) <= set(self.request):
            self._change_pi_for_object()
            return self.response

        if "observationPriority" in self.request:
            self._set_observational_priority_for_object()
            return self.response

        # throw warning if nothing has changed
        if len(self.response) == 0:
            self.response = "nothing has changed"

        self.log.debug('completed the ``get`` method')

        return self.response

    def _move_transient_to_another_list(
            self):
        """ create sqlquery for the put request
        """
        self.log.debug('starting the ``_create_sqlquery`` method')
        transientBucketId = self.transientBucketId

        sqlQuery = u"""
            select marshallWorkflowLocation, alertWorkflowLocation from pesstoObjects where transientBucketId = %(transientBucketId)s
        """ % locals()
        objectData = readquery(sqlQuery, self.dbConn, self.log)

        oldMwl = objectData[0]["marshallWorkflowLocation"]
        oldAwl = objectData[0]["alertWorkflowLocation"]
        username = self.request["authenticated_userid"].replace(".", " ").title()
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        # CHANGE THE MARSHALL WORKFLOW LOCATION LIST IF REQUESTED

        if "mwl" in self.request:
            mwl = self.request["mwl"]
            print(mwl)
            if "snoozed" in self.request:
                logEntry = "object snoozed by %(username)s" % locals(
                )
                snoozed = ", snoozed = 1"
            else:
                logEntry = "moved from '%(oldMwl)s' to '%(mwl)s' list by %(username)s" % locals(
                )
                snoozed = ", snoozed = 0"

            sqlQuery = """
                update pesstoObjects set marshallWorkflowLocation = "%(mwl)s" %(snoozed)s  where transientBucketId = %(transientBucketId)s
            """ % locals()
            writequery(self.log, sqlQuery, self.dbConn)
            self.response = self.response + \
                " transientBucketId %(transientBucketId)s moved to the `%(mwl)s` marshallWorkflowLocation<BR>" % locals(
                )

            for o, n in zip(["pending observation", "following", "pending classification"], ["classification targets", "followup targets", "queued for classification"]):
                logEntry = logEntry.replace(o, n)

            sqlQuery = u"""insert ignore into transients_history_logs (
                transientBucketId,
                dateCreated,
                log
            )
            VALUES (
                %s,
                "%s",
                "%s"
            )""" % (transientBucketId, now, logEntry)
            writequery(self.log, sqlQuery, self.dbConn)

            # RESET PRIORITY IF REQUIRED
            if mwl == "following":
                sqlQuery = """
                    update pesstoObjects set observationPriority = 2 where transientBucketId = %(transientBucketId)s
                """ % locals()
                writequery(self.log, sqlQuery, self.dbConn)

            # RESET THE LAST TIME REVIEWE IF REQUIRED
            if mwl == "archive":
                now = datetime.now()
                now = now.strftime("%Y-%m-%d %H:%M:%S")
                sqlQuery = """
                    update pesstoObjects set lastReviewedMagDate = "%(now)s" where transientBucketId = %(transientBucketId)s
                """ % locals()
                writequery(self.log, sqlQuery, self.dbConn)

        # CHANGE THE ALERT WORKFLOW LOCATION LIST IF REQUESTED
        if "awl" in self.request:
            awl = self.request["awl"]
            sqlQuery = """
                update pesstoObjects set alertWorkflowLocation = "%(awl)s", snoozed = 0 where transientBucketId = %(transientBucketId)s
            """ % locals()
            writequery(self.log, sqlQuery, self.dbConn)
            self.response = self.response + \
                " transientBucketId %(transientBucketId)s moved to the `%(awl)s` alertWorkflowLocation<BR>" % locals(
                )

            logEntry = "moved from '%(oldAwl)s' to '%(awl)s' list by %(username)s" % locals(
            )
            for o, n in zip(["pending observation", "following", "pending classification"], ["classification targets", "followup targets", "queued for classification"]):
                logEntry = logEntry.replace(o, n)
            sqlQuery = u"""insert ignore into transients_history_logs (
                transientBucketId,
                dateCreated,
                log
            )
            VALUES (
                %s,
                "%s",
                "%s"
            )""" % (transientBucketId, now, logEntry)
            writequery(self.log, sqlQuery, self.dbConn)

        self.log.debug('completed the ``_create_sqlquery`` method')
        return None

    def _change_pi_for_object(
            self):
        """ change pi for object
        """
        self.log.debug('starting the ``_change_pi_for_object`` method')

        piName = self.request["piName"]
        piEmail = self.request["piEmail"]
        transientBucketId = self.transientBucketId
        username = self.request["authenticated_userid"].replace(".", " ").title()
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        sqlQuery = """
            select pi_name, pi_email from pesstoObjects where transientBucketId = %(transientBucketId)s
        """ % locals()
        objectData =  readquery(sqlQuery, self.dbConn, self.log)
        oldPiName = objectData[0]["pi_name"]
        oldPiEmail = objectData[0]["pi_email"]

        # CHANGE THE PI IN THE DATABASE
        sqlQuery = """
            update pesstoObjects set pi_name = "%(piName)s", pi_email = "%(piEmail)s" where transientBucketId = %(transientBucketId)s
        """ % locals()
        writequery(self.log, sqlQuery, self.dbConn)

        self.response = self.response + \
            "changed the PI of transient #%(transientBucketId)s to '%(piName)s' (%(piEmail)s)" % locals(
            )

        if oldPiName:
            logEntry = "PI changed from %(oldPiName)s (%(oldPiEmail)s) to %(piName)s (%(piEmail)s) by %(username)s" % locals(
            )
        else:
            logEntry = "%(piName)s (%(piEmail)s) assigned as PI of this object by by %(username)s" % locals(
            )

        sqlQuery = u"""insert ignore into transients_history_logs (
            transientBucketId,
            dateCreated,
            log
        )
        VALUES (
            %s,
            "%s",
            "%s"
        )""" % (transientBucketId, now, logEntry)
        writequery(self.log, sqlQuery, self.dbConn)

        self.log.debug('completed the ``_change_pi_for_object`` method')
        return None

    def _set_observational_priority_for_object(
            self):
        """ change the observational priority for an object
        """
        self.log.debug(
            'completed the ````_set_observational_priority_for_object`` method')

        observationPriority = self.request[
            "observationPriority"].strip()
        transientBucketId = self.transientBucketId
        username = self.request["authenticated_userid"].replace(".", " ").title()
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        # GET OLD DATA
        sqlQuery = """
            select observationPriority, marshallWorkflowLocation from pesstoObjects where transientBucketId = %(transientBucketId)s
        """ % locals()
        objectData = readquery(sqlQuery, self.dbConn, self.log)

        oldobservationPriority = objectData[0]["observationPriority"]
        mwl = objectData[0]["marshallWorkflowLocation"]

        if observationPriority == False or observationPriority == "False":
            observationPriority = "null"

        # CHANGE THE OBSERVATION PRIORITY IN THE DATABASE
        sqlQuery = """
            update pesstoObjects set observationPriority = %(observationPriority)s where transientBucketId = %(transientBucketId)s
        """ % locals()
        writequery(self.log, sqlQuery, self.dbConn)

        # RESPONSE
        self.response = self.response + \
            "changed the observational priority of transient #%(transientBucketId)s to '%(observationPriority)s'" % locals(
            )

        if observationPriority == "null":
            self.log.debug(
                'completed the ``_set_observational_priority_for_object`` method')
            return None

        observationPriority = int(observationPriority)
        oldobservationPriority = int(oldobservationPriority)

        if mwl == "following":
            for n, w in zip([1, 2, 3, 4], ["CRITICAL", "IMPORTANT", "USEFUL", "NONE"]):
                if n == oldobservationPriority:
                    oldobservationPriority = w

            for n, w in zip([1, 2, 3, 4], ["CRITICAL", "IMPORTANT", "USEFUL", "NONE"]):
                if n == observationPriority:
                    observationPriority = w

            # LOG ENTRY
            logEntry = "observation priority changed from %(oldobservationPriority)s to %(observationPriority)s by %(username)s" % locals(
            )

        else:
            for n, w in zip([1, 2, 3], ["HIGH", "MEDIUM", "LOW"]):
                if n == oldobservationPriority:
                    oldobservationPriority = w

            for n, w in zip([1, 2, 3], ["HIGH", "MEDIUM", "LOW"]):
                if n == observationPriority:
                    observationPriority = w

            # LOG ENTRY
            logEntry = "classification priority changed from %(oldobservationPriority)s to %(observationPriority)s by %(username)s" % locals(
            )

        sqlQuery = u"""insert ignore into transients_history_logs (
            transientBucketId,
            dateCreated,
            log
        )
        VALUES (
            %s,
            "%s",
            "%s"
        )""" % (transientBucketId, now, logEntry)
        writequery(self.log, sqlQuery, self.dbConn)

        self.log.debug(
            'completed the ``_set_observational_priority_for_object`` method')
        return None

    # xt-class-method