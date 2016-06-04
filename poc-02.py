#!/usr/bin/python3
#
# https://github.com/OfflineIMAP/imapfw/wiki/sync-02
#
# TODO:
# [x] Merge the changes in the engine for messages having changes in both sides.
#     This allows to mark identical changes to avoid propagating them to the
#     driver.
# [x] Learn the state driver to only record successful updates.
# [ ] Learn deletions.
# [ ] Expose updates to the rascal.
# [ ] Turn into concurrent mode.


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
    def search(self):
        return self.data

class StateStorage(Storage):
    """Would run in a worker."""

    def update(self, message):
        """StateDriver Must Contain MetaData for last synced messages rather
        than full emails. For now we are putting full messages."""

        #TODO: we have to later think of its implementation and format.
        uid = message.getUID()
        if uid in self.data:
            # Update message in storage.
            storageMessage = self.data[uid]
            message.fakeStateWrites(storageMessage)
        else:
            self.data[uid] = message


#TODO: fake real drivers.
#TODO: Assign UID when storage is IMAP.
class Driver(Storage):
    """Fake a driver."""

    FakeDriverWriteError = False

    def __init__(self, name, *args, **kw):
        self.name = name
        super(Driver, self).__init__(*args, **kw)

    def fakeChange(self, message):
        """Add or erase with newMessage."""

        #FIXME: updates and new messages are handled. Not the deletions.
        self.data[message.uid] = deepcopy(message)

    def update(self, message):
        if self.FakeDriverWriteError is True:
            self.FakeDriverWriteError = False
            raise IOError("write by driver failed")
        uid = message.getUID()
        if uid in self.data:
            # Update message in storage.
            storageMessage = self.data[uid]
            message.fakeDriverWrites(storageMessage)
        else:
            self.data[uid] = message


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
                self.driver.update(theirMessage)
                self.state.update(theirMessage) # Would be async.
            except IOError as e:
                print("Write error on %s failed: %s"% (self.driver.name, e))

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
        state = StateStorage() # Would be an emitter.
        # Add the state controller to the chain of controllers of the drivers.
        # Real driver might need API to work on chained controllers.
        self.left = StateController(left, state)
        self.right = StateController(right, state)

    def debug(self, title):
        log(title)
        log("left:  %s"% self.left.driver.data)
        log("rght:  %s"% self.right.driver.data)
        log("state: %s"% self.left.state.data) # leftState == rightState
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
    # Fill both sides with pre-existing data.
    left = Driver("left")  # Fake those data.
    right = Driver("rght") # Fake those data.

    # Start engine.
    engine = Engine(left, right)

    log("\n# RUN 0")
    engine.debug("## Before RUN 0")
    engine.run()
    engine.debug("\n## After RUN 0.")
    log("\n# RUN 0 done")

    log("\n# RUN 1")
    m1 = Message(1, "1 body") # Same as m1r.
    m2 = Message(2, "2 body") # Same as m2r.
    left.fakeChange(m1)
    left.fakeChange(m2)
    engine.debug("## Before RUN 1")
    engine.run()
    engine.debug("\n## After RUN 1.")
    log("# RUN 1 done\n")


    log("\n# RUN 2 (different changes on same message)")
    m2.markImportant()
    right.fakeChange(m2)
    m2.unmarkImportant()
    m2.markRead()
    left.fakeChange(m2)
    engine.debug("## Before RUN 2")
    engine.run()
    engine.debug("\n## After RUN 2.")
    log("# RUN 2 done\n")


    log("\n# RUN 3 (same changes from both sides)")
    m2.unmarkImportant()
    m2.markRead()
    right.fakeChange(m2)
    left.fakeChange(m2)
    engine.debug("## Before RUN 3")
    engine.run()
    engine.debug("\n## After RUN 3.")
    log("# RUN 3 done\n")


    log("\n# RUN 4 (left write error)")
    m2.markImportant()
    right.fakeChange(m2)
    left.FakeDriverWriteError = True
    engine.debug("## Before RUN 4")
    engine.run()
    engine.debug("\n## After RUN 4.")
    log("# RUN 4 done\n")

    log("\n# RUN 5 (sync after left write error)")
    engine.debug("## Before RUN 5")
    engine.run()
    engine.debug("\n## After RUN 5.")
    log("# RUN 5 done\n")


    log("\n# RUN 6 (changes from both sides, left write fails)")
    m2.unmarkImportant()
    right.fakeChange(m2)
    m2.markImportant()
    m2.unmarkRead()
    left.fakeChange(m2)
    left.FakeDriverWriteError = True
    engine.debug("## Before RUN 6")
    engine.run()
    engine.debug("\n## After RUN 6.")
    log("# RUN 6 done\n")

    log("\n# RUN 7 (sync after left write fails)")
    engine.debug("## Before RUN 7")
    engine.run()
    engine.debug("\n## After RUN 7.")
    log("# RUN 7 done\n")

    #TODO: PASS with changed messages.


    log("\n# LAST RUN (no changes)")
    engine.debug("## Before LAST RUN")
    engine.run()
    engine.debug("\n## After LAST RUN.")
    log("# LAST RUN done\n")
