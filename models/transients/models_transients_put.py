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
from fundamentals import times

from astrocalc.times import conversions
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
        if "mwl" in self.request or "awl" in self.request or "snoozed" in self.request:
            self._move_transient_to_another_list()
            self._refresh_sidebar_list_counts()
            return self.response

        # change the pi is requested
        if set(("piName", "piEmail")) <= set(self.request):
            self._change_pi_for_object()
            self._refresh_sidebar_list_counts()
            return self.response

        if "observationPriority" in self.request:
            self.response = "Cannot change priority from this endpoint. Priorities should be given via Scheduler API."
            return self.response
        
        if "clsType" in self.request:
            self._add_transient_classification()
            self._refresh_sidebar_list_counts()
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
        print("Moving the transient to another list")
        self.log.debug('starting the ``_create_sqlquery`` method')
        transientBucketId = self.transientBucketId

        sqlQuery = u"""
            select marshallWorkflowLocation, alertWorkflowLocation, snoozed from pesstoObjects where transientBucketId = %(transientBucketId)s
        """ % locals()
        objectData = readquery(sqlQuery, self.dbConn, self.log)

        oldMwl = objectData[0]["marshallWorkflowLocation"]
        print(f"The old mwl is {oldMwl}")
        oldAwl = objectData[0]["alertWorkflowLocation"]
        oldSnoozed = objectData[0]["snoozed"]
        username = self.request["authenticated_userid"].replace(".", " ").title()
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        ## TODO: Controlla qua se c'è un snoozed.
        if "snoozed" in self.request and self.request["snoozed"] == True:
            if oldMwl.lower() == "inbox":
                print("Snoozing the object")
                logEntry = "object snoozed by %(username)s" % locals(
                )
                snoozed = ", snoozed = 1"
                sqlQuery = """
                    update pesstoObjects set snoozed = 1, marshallWorkflowLocation = "archive" where transientBucketId = %(transientBucketId)s
                """ % locals()
                writequery(self.log, sqlQuery, self.dbConn)

                #NOW UPDATING THE LOG
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



                self.response = self.response + \
                        " transientBucketId %(transientBucketId)s snoozed by %(username)s<BR>" % locals(
                    )



                self.log.debug('completed the ``_create_sqlquery`` method')
                return None
            else:
                self.response = self.response + \
                    " transientBucketId %(transientBucketId)s cannot be snoozed as it is not in the `inbox` list<BR>" % locals(
                    )
                self.log.debug('completed the ``_create_sqlquery`` method')
                raise ValueError("Invalid marshallWorkflowLocation")
                return None


        if "snoozed" in self.request and self.request["snoozed"] == False:
            if oldSnoozed == True:
            # Check that the object is really snoozed.

                sqlQuery = """
                    update pesstoObjects set snoozed = 0, marshallWorkflowLocation = "inbox" where transientBucketId = %(transientBucketId)s
                """ % locals()
                writequery(self.log, sqlQuery, self.dbConn)
                logEntry = "object unsnoozed by %(username)s" % locals(
                )

                #NOW UPDATING THE LOG
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



                self.response = self.response + \
                        " transientBucketId %(transientBucketId)s unsnoozed by %(username)s<BR>" % locals(
                    )
                return None
            else:
                self.response = self.response + \
                    " transientBucketId %(transientBucketId)s cannot be unsnoozed as it is not in snoozed." % locals(
                    )
                raise ValueError("Invalid marshallWorkflowLocation")
                return None




        if "mwl" in self.request:
            mwl = self.request["mwl"]
            print(mwl)

            # VALIDATE THE MOVE
            if mwl not in ["inbox", "pending observation", "review for followup", "following", "followup complete", "archive"]:
                self.response = self.response + \
                    " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it is not a valid list<BR>" % locals(
                    )
                self.log.debug('completed the ``_create_sqlquery`` method')
                raise ValueError("Invalid marshallWorkflowLocation")
                return None
                
            # CHECK THE OLD WORKFLOW LOCATION AND OTHER STUFF NEEDED (AS PI OR CLASSIFICATION) IN ORDER TO MOVE CORRECTLY

            if oldMwl == "Inbox":
                # THE ONLY POSSIBLE MOVE IS ARCHIVE
                print("Here")
                if mwl != "archive":
                    self.response = self.response + \
                        " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it is in the `inbox` list<BR>" % locals(
                        )
                    self.log.debug('completed the ``_create_sqlquery`` method')
                    raise ValueError("Invalid marshallWorkflowLocation")
                    return None
            
            if oldMwl == "pending observation":
                # THE POSSIBLE MOVES ARE REVIEW FOR FOLLOWUP OR ARCHIVE. REVIEW FOR FOLLOWUP REQUIRES classifiedFlag = True
                if mwl == "review for followup":
                    sqlQuery = u"""
                        select classifiedFlag from pesstoObjects where transientBucketId = %(transientBucketId)s
                    """ % locals()
                    objectData = readquery(sqlQuery, self.dbConn, self.log)
                    classifiedFlag = objectData[0]["classifiedFlag"]
                    if not classifiedFlag:
                        self.response = self.response + \
                            " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it is not classified<BR>" % locals(
                            )
                        self.log.debug('completed the ``_create_sqlquery`` method')
                        raise ValueError("Invalid marshallWorkflowLocation")
                        return None
                elif mwl == "archive":
                    pass
                else:
                    self.response = self.response + \
                        " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it is in the `pending observation` list<BR>" % locals(
                        )           
                    self.log.debug('completed the ``_create_sqlquery`` method')
                    raise ValueError("Invalid marshallWorkflowLocation")
                    return None 
            if oldMwl == "review for followup":
                # THE ONLY POSSIBLE MOVE IS FOLLOWUP IF THE PI IS SET OR ARCHIVE
                if mwl == "following":
                    sqlQuery = u"""
                        select pi_name, pi_email from pesstoObjects where transientBucketId = %(transientBucketId)s
                    """ % locals()
                    objectData = readquery(sqlQuery, self.dbConn, self.log)
                    piName = objectData[0]["pi_name"]
                    piEmail = objectData[0]["pi_email"]
                    if not piName or not piEmail:
                        self.response = self.response + \
                            " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it does not have a PI assigned<BR>" % locals(
                            )
                        self.log.debug('completed the ``_create_sqlquery`` method')
                        raise ValueError("Invalid marshallWorkflowLocation")
                        return None
                elif mwl == "archive":
                    pass
                else:
                    self.response = self.response + \
                        " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it is in the `review for followup` list<BR>" % locals(
                        )           
                    self.log.debug('completed the ``_create_sqlquery`` method')
                    raise ValueError("Invalid marshallWorkflowLocation")
                    return None 
            
            if oldMwl == "following":
                # THE ONLY POSSIBLE MOVE IS FOLLOWUP COMPLETE
                if mwl != "followup complete" :
                    self.response = self.response + \
                        " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it is in the `following` list<BR>" % locals(
                        )
                    self.log.debug('completed the ``_create_sqlquery`` method')
                    raise ValueError("Invalid marshallWorkflowLocation")
                    return None
            
            if oldMwl == "followup complete":
                # THE ONLY POSSIBLE MOVE IS ARCHIVE
                if mwl != "archive" :
                    self.response = self.response + \
                        " transientBucketId %(transientBucketId)s cannot be moved to the `%(mwl)s` marshallWorkflowLocation as it is in the `followup complete` list<BR>" % locals(
                        )
                    self.log.debug('completed the ``_create_sqlquery`` method')
                    raise ValueError("Invalid marshallWorkflowLocation")
                    return None


            
            sqlQuery = """
                update pesstoObjects set marshallWorkflowLocation = "%(mwl)s"   where transientBucketId = %(transientBucketId)s
            """ % locals()
            writequery(self.log, sqlQuery, self.dbConn)
            self.response = self.response + \
                " transientBucketId %(transientBucketId)s moved to the `%(mwl)s` marshallWorkflowLocation<BR>" % locals(
                )
            logEntry = " transientBucketId %(transientBucketId)s moved to the `%(mwl)s` marshallWorkflowLocation<BR> by %(username)s" % locals(
                )
            now = datetime.now()
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


            # RESET THE LAST TIME REVIEWED IF REQUIRED
            ## TODO Recheck action on Archive here

            if mwl == "archive":
                now = datetime.now()
                now = now.strftime("%Y-%m-%d %H:%M:%S")
                sqlQuery = """
                    update pesstoObjects set lastReviewedMagDate = "%(now)s" where transientBucketId = %(transientBucketId)s
                """ % locals()
                writequery(self.log, sqlQuery, self.dbConn)

                #DELETE ALL THE ASSOCIATED OB USING THE SCHEDULER API (TBD TODO)


        # CHANGE THE ALERT WORKFLOW LOCATION LIST IF REQUESTED (STILL TBD TODO)
        if "awl" in self.request:
            awl = self.request["awl"]

            if awl == "queued for atel":
                #Shoud we check for the marshall location ? If this is the case, then do it here
                sqlQuery = """
                    update pesstoObjects set alertWorkflowLocation = "%(awl)s", snoozed = 0 where transientBucketId = %(transientBucketId)s
                """ % locals()
                writequery(self.log, sqlQuery, self.dbConn)
                self.response = self.response + \
                    " transientBucketId %(transientBucketId)s moved to the `%(awl)s` alertWorkflowLocation" % locals(
                    )

                logEntry = "moved from '%(oldAwl)s' to '%(awl)s' list by %(username)s" % locals(
                )
                return None
            if awl == "released" or awl == "not to be released":
                if oldAwl == "queued for atel":
                    sqlQuery = """
                    update pesstoObjects set alertWorkflowLocation = "%(awl)s", snoozed = 0 where transientBucketId = %(transientBucketId)s
                    """ % locals()
                    writequery(self.log, sqlQuery, self.dbConn)
                    self.response = self.response + \
                        " transientBucketId %(transientBucketId)s moved to the `%(awl)s` alertWorkflowLocation" % locals(
                     )

                    logEntry = "moved from '%(oldAwl)s' to '%(awl)s' list by %(username)s" % locals(
                    )
                    return None
                else:
                    raise ValueError("Invalid marshallAlertLocation")



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

        # ALLOW TO CHANGE THE PI ONLY IF THE OBJECT IS IN THE REVIEW FOR FOLLOWUP OR FOLLOWING LIST OR PI HAS ALREADY BEEN SET

        sqlQuery = """
            select pi_name, pi_email, marshallWorkflowLocation from pesstoObjects where transientBucketId = %(transientBucketId)s
        """ % locals()
        objectData =  readquery(sqlQuery, self.dbConn, self.log)
        oldPiName = objectData[0]["pi_name"]
        oldPiEmail = objectData[0]["pi_email"]
        mwl = objectData[0]["marshallWorkflowLocation"]

        if mwl not in ["review for followup", "following"] and (not oldPiName or not oldPiEmail):
            self.response = self.response + \
                " transientBucketId %(transientBucketId)s cannot have the PI changed as it is in the `%(mwl)s` list and does not have a PI assigned<BR>" % locals(
                )
            self.log.debug('completed the ``_change_pi_for_object`` method')
            raise ValueError("Invalid marshallWorkflowLocation or transient does not have a PI assigned")
            return None

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
        ## TODO: recheck priorities

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

    def _add_transient_classification(
            self):
        """ add transient classification
        """
        self.log.debug('starting the ``_add_transient_classification`` method')

        transientBucketId = self.transientBucketId

        # ASTROCALC CONVERTER
        converter = conversions(
            log=self.log
        )

        # FIRST SELECT OUT A ROW FROM THE TRANSIENTBUCKET AS A TEMPLATE FOR THE
        # CLASSIFICATION
        
        sqlQuery = """
            select p.classifiedFlag, t.raDeg, t.decDeg, t.name from transientBucket t, pesstoObjects p where replacedByRowId = 0 and t.transientBucketId = %(transientBucketId)s and t.transientBucketId=p.transientBucketId limit 1
        """ % locals()
        rows = readquery(sqlQuery, self.dbConn, self.log)


        # ADD VARIABLES TO a
        params = dict(list(self.request.items()))
        params["now"] = times.get_now_sql_datetime()
        params["transientBucketId"] = self.transientBucketId
        for row in rows:
            params["raDeg"] = row["raDeg"]
            params["decDeg"] = row["decDeg"]
            params["name"] = row["name"]
            params["classifiedFlag"] = row["classifiedFlag"]

        # CONVERT DATE TO MJD
        params["obsMjd"] = converter.ut_datetime_to_mjd(
            utDatetime="%(clsObsdate)sT00:00:00.0" % params)

        # ADD SOME DEFAULT VALUES / NULL VALUES
        if "clsPeculiar" in params and params["clsType"] == True:
            params[
                "clsSnClassification"] = "%(clsSnClassification)s-p" % params
        if params["clsType"] == "supernova":
            params["clsType"] = "SN " + params["clsSnClassification"]
        if "clsRedshift" not in params or len(params["clsRedshift"]) == 0:
            params["clsRedshift"] = "null"
        if "clsClassificationWRTMax" not in params:
            params["clsClassificationWRTMax"] = "unknown"
        if "clsClassificationPhase" not in params or len(params["clsClassificationPhase"]) == 0:
            params["clsClassificationPhase"] = "null"

        params["username"] = self.request["authenticated_userid"].replace(".", " ").title()
        print(params)
        
        # INSERT THE NEW CLASSIFICATION ROW INTO THE TRANSIENTBUCKET
        sqlQuery = """
                INSERT IGNORE INTO transientBucket (raDeg, decDeg, name, transientBucketId, observationDate, observationMjd, survey, spectralType, transientRedshift, classificationWRTMax, classificationPhase, reducer) VALUES(%(raDeg)s, %(decDeg)s, "%(name)s", %(transientBucketId)s, "%(clsObsdate)s", %(obsMjd)s, "%(clsSource)s", "%(clsType)s", %(clsRedshift)s, "%(clsClassificationWRTMax)s", %(clsClassificationPhase)s, "%(username)s") ON DUPLICATE KEY UPDATE raDeg = values(raDeg), decDeg = values(decDeg), name = values(name), transientBucketId = values(transientBucketId), observationDate = values(observationDate), observationMjd = values(observationMjd), survey = values(survey), spectralType = values(spectralType), transientRedshift = values(transientRedshift), classificationWRTMax = values(classificationWRTMax), classificationPhase = values(classificationPhase), reducer=values(reducer);
            """ % params

        #WRITE THE QUERY
        writequery(self.log, sqlQuery, self.dbConn)
        
        # UPDATE THE OBJECT'S LOCATION IN THE VARIOUS MARSHALL WORKFLOWS

        if  params["clsSendTo"].lower() == "yes":
            awl = "queued for atel"
        else:
            awl = "archived without alert"


        if params["classifiedFlag"] == 1:
            sqlQuery = """
                update pesstoObjects set snoozed = 0, alertWorkflowLocation = "%(awl)s" where transientBucketId = %(transientBucketId)s
            """ % locals()
        else:
            sqlQuery = """
                update pesstoObjects set classifiedFlag = 1, snoozed = 0, marshallWorkflowLocation = "review for followup", alertWorkflowLocation = "%(awl)s" where transientBucketId = %(transientBucketId)s
            """ % locals()
        writequery(self.log, sqlQuery, self.dbConn)

        # RUN THE STORED PROCDURE TO UPDATE THE PESSTO OBJECTS TABLE
        sqlQuery = f"call update_single_transientbucket_summary({self.transientBucketId})"
        writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.dbConn
            )
        

        self.response = self.response + \
            "Add a classification to transient #%(transientBucketId)s " % locals(
            )

        self.log.debug(
            'completed the ``_add_transient_classification`` method')
        return None

    def _refresh_sidebar_list_counts(self):
        self.log.debug('starting the ``put`` method')

        # all marshall workflow list titles
        marshallWorkflowLists = ["inbox", "archive", "following", "pending observation",
                                 "followup complete", "review for followup", "pending classification"]

        # count objects in each list and update the `meta_workflow_lists_counts`
        # table
        for thisList in marshallWorkflowLists:
            sqlListName = thisList.replace(" ", "_")
            sqlQuery = """update meta_workflow_lists_counts set count = (select count(*) from pesstoObjects where marshallWorkflowLocation="%(thisList)s") where listname = "%(thisList)s" """ % locals(
            )
            writequery(self.log, sqlQuery, self.dbConn)


        # all alert workflow list titles
        alertWorkflowLists = [
            "external alert released", "pessto classification released", "archived without alert", "queued for atel"]

        # count objects in each list and update the `meta_workflow_lists_counts`
        # table
        for thisList in alertWorkflowLists:
            sqlListName = thisList.replace(" ", "_")
            sqlQuery = """update meta_workflow_lists_counts set count = (select count(*) from pesstoObjects where alertWorkflowLocation="%(thisList)s") where listname = "%(thisList)s" """ % locals(
            )
            writequery(self.log, sqlQuery, self.dbConn)

        # count all objects
        sqlQuery = """update meta_workflow_lists_counts set count = (select count(*) from pesstoObjects) where listname = "all" """ % locals(
        )
        writequery(self.log, sqlQuery, self.dbConn)


        # count classified objects
        sqlQuery = """update meta_workflow_lists_counts set count = (select count(*) from pesstoObjects where classifiedFlag = 1) where listname = "classified" """ % locals(
        )
        writequery(self.log, sqlQuery, self.dbConn)


        # count snoozed objects
        sqlQuery = """update meta_workflow_lists_counts set count = (select count(*) from pesstoObjects where snoozed = 1) where listname = "snoozed" """ % locals(
        )
        writequery(self.log, sqlQuery, self.dbConn)


        self.log.debug('completed the ``put`` method')


    # xt-class-method