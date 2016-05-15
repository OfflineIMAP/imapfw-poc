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
        self.flags = {'read': False, 'important': False}
        # Store what was changed since previous sync. Flags can be:
        # - True: addition
        # - False: deletion
        # - None: no change
        self.changes = self.DEFAULT_CHANGES

    def __repr__(self):
        return "<Message %s [%s] '%s'>"% (self.uid, self.flags, self.body)

    def __eq__(self, other):
        return self.uid == other

    def __hash__(self):
        return hash(self.uid)

    def __lt__(self, other):
        return self.uid < other

    def fakeWrite(self):
        """Fake applying changes when written to a storage."""

        for flag, value in self.changes.items():
            if value is not None:
                self.flags[flag] = self.changes[flag]
        self.changes = self.DEFAULT_CHANGES

    def getChanges(self):
        return self.changes

    def getFlags(self):
        return self.flags

    def getUID(self):
        return self.uid

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

    def merge(self, message):
        """Merge changes from message."""

        assert message.getUID() == self.uid

        for flag, change in message.getChanges().items():
            # Flags with None change did not changed.
            if change is not None:
                current = self.changes[flag]
                if current is None and change != current:
                    self.changes[flag] = change
                    log("->  Merging change %s: %s: %s"% (self, flag, change))

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

        # From theirs to ours.
        for uid, theirMessage in theirMessages.items():
            if uid in self.data:
                self.data[uid].merge(theirMessage)

        # From ours to theirs.
        for uid, message in self.items():
            if uid in theirMessages.data:
                theirMessages.data[uid].merge(message)


# Fake any storage. Allows making this PoC more simple.
class Storage(UserDict):
    def __init__(self, dict_messages):
        self.messages = Messages(dict_messages) # Fake the real data.

    def search(self):
        return self.messages

    def write(self, newMessage):
        """Update the storage.

        newMessage: messages we have to create, update or remove."""

        #FIXME: If we provide try to update leftside first it will be updated as
        # per right side changes provided by Engine.  For Example: m2l is marked
        # read and m2r is unread. Now when we are first trying to update left
        # side with changes. We are making it unread on both the side.  Where as
        # if we first update right side we will have m2l as well as m2r as read.
        # same goes for removal. Lets say there is m4l but m4r is missing . So
        # what should we do should we remove m4l or what.

        #FIXME: updates and new messages are handled. Not the deletions.
        newMessage.fakeWrite()
        self.messages.add(newMessage)


class StateDriver(Storage):
    """Would run in a worker."""

    def write(self, message):
        """StateDriver Must Contain MetaData for last synced messages rather
        than full emails. For now we are putting full messages."""

        #TODO: we have to later think of its implementation and format.
        super(StateDriver, self).write(message)


#TODO: fake real drivers.
#TODO: Assign UID when storage is IMAP.
class Driver(Storage):
    """Fake a driver."""
    pass


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
                changedMessages.add(message)

        # TODO: mark message as destroyed from real repository.
        # for stateMessage in stateMessages:
            # if stateMessage not in messages:
                # changedMessages.add(message)

        return changedMessages


class Engine(object):
    """The engine."""
    def __init__(self, left, right):
        state = StateDriver([]) # Would be an emitter.
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
        log("- from left: %s"% leftMessages)
        log("- from rght: %s"% rightMessages)

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

    leftMessages = Messages({m1l.uid: m1l, m2l.uid: m2l})
    rghtMessages = Messages({m1r.uid: m1r, m2r.uid: m2r, m3r.uid: m3r})

    # Fill both sides with pre-existing data.
    left = Driver(leftMessages) # Fake those data.
    right = Driver(rghtMessages) # Fake those data.

    # Start engine.
    engine = Engine(left, right)

    log("\n# RUN 1")
    engine.debug("## Before RUN 1")
    engine.run()
    engine.debug("\n## After RUN 1.")
    log("\n# RUN 1 done")


    log("\n# RUN 2")
    m2r.markImportant()
    right.write(m2r)
    m2l.markRead()
    left.write(m2l)
    engine.debug("## Before RUN 2")
    engine.run()
    engine.debug("\n## After RUN 2.")
    log("\n# RUN 2 done")

    #TODO: PASS with changed messages.


    log("\n# LAST RUN (no changes)")
    engine.debug("## Before RUN 2")
    engine.run()
    engine.debug("\n## After RUN 2.")
    log("\n# LAST RUN done")
