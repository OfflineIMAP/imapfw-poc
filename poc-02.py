#!/usr/bin/python3
#
# https://github.com/OfflineIMAP/imapfw/wiki/sync-02
#
# TODO:
# 1. Merge the changes in the engine for messages having changes in both sides.
#    This allows to mark identical changes to avoid propagating them to the
#    driver.
# 2. Learn the state driver to only record successful updates.
# 3. Learn deletions.
# 4. Expose updates to the rascal.
# 5. Turn into concurrent mode.


from functools import total_ordering
from collections import UserDict
from copy import deepcopy

def log(*whatever):
    print(*whatever)

@total_ordering
class Message(object):
    """Fake the real Message class."""

    DEFAULT_CHANGES = {'read': None, 'important': None}

    def __init__(self, uid=None, body=None):
        self.uid = uid
        self.body = body

        self.unkown = False # This is a new message.
        self.flags = {'read': False, 'important': False}
        # Store what was changed since previous sync. Flags can be:
        # - True: addition
        # - False: deletion
        # - None: no change
        self.changes = self.DEFAULT_CHANGES
        # Update the state only with those changes.
        self.stateChanges = self.DEFAULT_CHANGES

    def __repr__(self):
        return "<Message %s [%s] '%s'>"% (self.uid, self.flags, self.body)

    def __eq__(self, other):
        return self.uid == other

    def __hash__(self):
        return hash(self.uid)

    def __lt__(self, other):
        return self.uid < other

    def fakeDriverWrites(self, storageMessage):
        """Fake applying changes when written to a driver."""

        if self.unkown is False:
            for flag, value in self.changes.items():
                if value is not None:
                    storageMessage.flags[flag] = self.changes[flag]

    def fakeStateWrites(self, storageMessage):
        """Fake applying changes when written to the state."""

        if self.unkown is False:
            for flag, value in self.changes.items():
                if value is not None:
                    storageMessage.flags[flag] = self.changes[flag]

            for flag, value in self.stateChanges.items():
                if value is not None:
                    storageMessage.flags[flag] = self.stateChanges[flag]

    def getChanges(self):
        return self.changes

    def getFlags(self):
        return self.flags

    def getUID(self):
        return self.uid

    def hasChanges(self):
        """Changes must be written by the driver."""

        if self.unkown is True:
            return True

        for change in self.changes.values():
            if change is not None:
                return True
        return False

    def identical(self, message):
        """Compare the flags."""

        assert message.uid == self.uid

        if message.flags != self.flags:
            return False

        return True # Identical

    def learnChanges(self, stateMessage):
        """Learn what was changed since stateMessage."""

        for flag, value in stateMessage.getFlags().items():
            if value != self.flags[flag]:
                log("-> Learning change %s: %s: %s"%
                    (self, flag, self.flags[flag]))
                self.changes[flag] = self.flags[flag]

    def markImportant(self):
        self.flags['important'] = True

    def markRead(self):
        self.flags['read'] = True

    def markUnkown(self):
        self.unkown = True

    def merge(self, message):
        """Ignore changes identical in both sides for the drivers."""

        assert message.getUID() == self.uid

        otherChanges = deepcopy(message.getChanges())
        ourChanges = deepcopy(self.changes)

        for flag, change in otherChanges.items():
            current = self.changes[flag]
            # "change is None" means it did not change since previous sync.
            if change is not None or current is not None:
                if change == current:
                    # Driver already have this change! Remove the change for the
                    # drivers and only update the state.
                    log("-> Ignoring change {%s: %s} from both sides"
                        "for driver"% (flag, change))
                    self.changes[flag] = None
                    message.changes[flag] = None
                    self.stateChanges[flag] = current

    def setDeleted(self):
        self.flags['deleted'] = True

    def unmarkImportant(self):
        self.flags['important'] = False

    def unmarkRead(self):
        self.flags['read'] = False


class Messages(UserDict):
    """Enable collections of messages the easy way."""

    def add(self, message):
        """Add or erase message."""

        self.data[message.uid] = deepcopy(message)

    def merge(self, theirMessages):
        """Merge the changes."""

        # Merge applies to both sides.
        for uid, theirMessage in theirMessages.items():
            if uid in self.data:
                self.data[uid].merge(theirMessage)


# Fake any storage. Allows making this PoC more simple.
class Storage(UserDict):
    def __init__(self, dict_messages):
        self.messages = Messages(dict_messages) # Fake the real data.

    def search(self):
        return self.messages

    def write(self, newMessage):
        """Update the storage.

        newMessage: messages we have to create, update or remove."""

        #FIXME: updates and new messages are handled. Not the deletions.
        self.messages.add(newMessage)

class StateStorage(Storage):
    """Would run in a worker."""

    def write(self, message):
        """StateDriver Must Contain MetaData for last synced messages rather
        than full emails. For now we are putting full messages."""

        #TODO: we have to later think of its implementation and format.
        uid = message.getUID()
        if uid in self.messages:
            # Update message in storage.
            storageMessage = self.messages[uid]
            message.fakeStateWrites(storageMessage)
        else:
            super(StateStorage, self).write(message)


#TODO: fake real drivers.
#TODO: Assign UID when storage is IMAP.
class Driver(Storage):
    """Fake a driver."""

    def __init__(self, name, *args, **kw):
        self.name = name
        super(Driver, self).__init__(*args, **kw)

    def writeFromOutside(self, message):
        super(Driver, self).write(message)

    def write(self, message):
        uid = message.getUID()
        if uid in self.messages:
            # Update message in storage.
            storageMessage = self.messages[uid]
            message.fakeDriverWrites(storageMessage)
        else:
            if message.hasChanges() is True:
                super(Driver, self).write(message)
            else:
                log(" -> %s: changes ignored for %s (already up-to-date)"%
                    (self.name, message))


class StateController(object):
    """State controller for a driver.

    Notice each state controller owns a driver and is the stranger of the other
    side.

    The state controller is supposed to communicate with:
        - our driver;
        - the engine;
        - our state backend (read-only);
        - their state backend.
    """

    def __init__(self, driver, state):
        self.driver = driver # The driver we own.
        self.state = state

    def update(self, theirMessages):
        """Update this side with the messages from the other side."""

        for theirMessage in theirMessages.values():
            try:
                self.driver.write(theirMessage)
                self.state.write(theirMessage) # Would be async.
            except:
                raise # Would handle or warn.

    #FIXME: we are lying around. The real search() should return full
    # messages or have parameter to set what we request exactly.
    # For the sync we need to know what was changed.
    def getChanges(self):
        """Explore our messages. Only return changes since previous sync."""

        changedMessages = Messages() # Collection of new, deleted and updated messages.
        messages = self.driver.search() # Would be async.
        stateMessages = self.state.search() # Would be async.

        for uid, message in messages.items():
            if uid in stateMessages:
                stateMessage = stateMessages[uid]
                if not message.identical(stateMessage):
                    message.learnChanges(stateMessage)
                    changedMessages.add(message)
            else:
                # Missing in the other side.
                message.markUnkown()
                changedMessages.add(message)

        # TODO: mark message as destroyed from real repository.
        # for stateMessage in stateMessages:
            # if stateMessage not in messages:
                # changedMessages.add(message)

        return changedMessages


class Engine(object):
    """The engine."""
    def __init__(self, left, right):
        state = StateStorage([]) # Would be an emitter.
        # Add the state controller to the chain of controllers of the drivers.
        # Real driver might need API to work on chained controllers.
        self.left = StateController(left, state)
        self.right = StateController(right, state)

    def debug(self, title):
        log(title)
        log("left:  %s"% self.left.driver.messages)
        log("rght:  %s"% self.right.driver.messages)
        log("state: %s"% self.left.state.messages) # leftState == rightState
        log("")

    def run(self):
        leftMessages = self.left.getChanges() # Would be async.
        rightMessages = self.right.getChanges() # Would be async.

        # Merge the changes.
        leftMessages.merge(rightMessages)

        log("\n## Changes found:")
        log("- from left: %s"% list(leftMessages.data.keys()))
        log("- from rght: %s"% list(rightMessages.data.keys()))

        self.left.update(rightMessages)
        self.right.update(leftMessages)


if __name__ == '__main__':

    # Messages
    m1r = Message(1, "1 body")
    m1l = Message(1, "1 body") # Same as m1r.

    m2r = Message(2, "2 body")
    m2l = Message(2, "2 body") # Same as m2r.
    # m2l.markRead()              # Same as m2r but read.

    m3r = Message(3, "3 body") # Not at left.

    # m4l = Message(None, "4 body") # Not at right.

    #leftMessages = Messages({m1l.uid: m1l, m2l.uid: m2l})
    # TODO: start empty for now.
    leftMessages = Messages()
    rghtMessages = Messages({m1r.uid: m1r, m2r.uid: m2r})

    # Fill both sides with pre-existing data.
    left = Driver("left", leftMessages) # Fake those data.
    right = Driver("rght", rghtMessages) # Fake those data.

    # Start engine.
    engine = Engine(left, right)

    log("\n# RUN 1")
    engine.debug("## Before RUN 1")
    engine.run()
    engine.debug("\n## After RUN 1.")
    log("\n# RUN 1 done")


    log("\n# RUN 2 (different changes on same message)")
    m2r.markImportant()
    right.writeFromOutside(m2r)
    m2l.markRead()
    left.writeFromOutside(m2l)
    engine.debug("## Before RUN 2")
    engine.run()
    engine.debug("\n## After RUN 2.")
    log("\n# RUN 2 done")


    log("\n# RUN 3 (same changes from both sides)")
    m2r.unmarkImportant()
    m2r.markRead()
    right.writeFromOutside(m2r)
    left.writeFromOutside(m2r)
    engine.debug("## Before RUN 3")
    engine.run()
    engine.debug("\n## After RUN 3.")
    log("\n# RUN 3 done")

    #TODO: PASS with changed messages.


    log("\n# LAST RUN (no changes)")
    engine.debug("## Before RUN 2")
    engine.run()
    engine.debug("\n## After RUN 2.")
    log("\n# LAST RUN done")
