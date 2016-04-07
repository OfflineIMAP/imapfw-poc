#!/usr/bin/python3
#
# https://github.com/OfflineIMAP/imapfw/wiki/sync-02


from functools import total_ordering
from collections import UserList
from copy import deepcopy


@total_ordering
class Message(object):
    """Fake the real Message class."""

    def __init__(self, uid=None, body=None):
        self.uid = uid
        self.body = body
        self.flags = {'read': False, 'important': False}
        # Store what was changed. Flags can be:
        # - True: addition
        # - False: deletion
        # - None: no change
        self.changes = {'read': None, 'important': None, 'deleted': False}

    def __repr__(self):
        return "<Message %s [%s] '%s'>"% (self.uid, self.flags, self.body)

    def __eq__(self, other):
        return self.uid == other

    def __hash__(self):
        return hash(self.uid)

    def __lt__(self, other):
        return self.uid < other

    def getChanges(self):
        return self.changes

    def identical(self, message):
        if message.uid != self.uid:
            return False
        if message.body != self.body:
            return False
        if message.flags != self.flags:
            return False

        return True # Identical

    def isImportant(self):
        return self.flags['important']

    def isRead(self):
        return self.flags['read']

    def learnChanges(self, stateMessage):
        """Learn what was changed is which way."""

        if self.isImportant() != stateMessage.isImportant():
            self.changes['important'] = self.isImportant()

        if self.isRead() != stateMessage.isRead():
            self.changes['read'] = self.isRead()

    def markImportant(self):
        self.flags['important'] = True

    def markRead(self):
        self.flags['read'] = True

    def setDeleted(self):
        self.changes['deleted'] = True

    def unmarkImportant(self):
        self.flags['important'] = False

    def unmarkRead(self):
        self.flags['read'] = False


class Messages(UserList):
    """Enable collections of messages the easy way."""
    pass #TODO: implement collection comparison.


# Fake any storage. Allows making this PoC more simple.
class Storage(object):
    def __init__(self, list_messages):
        self.messages = Messages(list_messages) # Fake the real data.

    def search(self):
        return self.messages

    def update(self, newMessage):
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

        if newMessage not in self.messages:
            self.messages.append(deepcopy(newMessage))
            return

        for message in self.messages:
            if message.uid == newMessage.uid:
                # Update message.
                message.body = newMessage.body
                message.flags = newMessage.flags
                return

        assert("should never hit this point")


class StateDriver(Storage):
    """Would run in a worker."""

    def update(self, message):
        """StateDriver Must Contain MetaData for last synced messages rather
        than full emails. For now we are putting full messages."""

        #TODO: we have to later think of its implementation and format.
        super(StateDriver, self).update(message)


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

    def __init__(self, driver, ourState, theirState):
        self.driver = driver # The driver we own.
        self.state = ourState
        self.theirState = theirState

    def update(self, theirMessages):
        """Update this side with the messages from the other side."""

        for theirMessage in theirMessages:
            try:
                self.driver.update(theirMessage)
                self.theirState.update(theirMessage) # Would be async.
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

        #TODO: we have to read both states to know what to sync. The current
        # implementation is wrong.
        for message in self.theirState.search():
            if message not in stateMessages:
                stateMessages.append(deepcopy(message))

        for message in messages:
            if message not in stateMessages:
                # Missing in the other side.
                changedMessages.append(deepcopy(message))
            else:
                for stateMessage in stateMessages:
                    if message.uid == stateMessage.uid:
                        if not message.identical(stateMessage):
                            changedMessages.append(deepcopy(message))
                            break #There is no point of iterating further.

        for stateMessage in stateMessages:
            if stateMessage not in messages:
                # TODO: mark message as destroyed from real repository.
                changedMessages.append(deepcopy(message))

        return changedMessages


class Engine(object):
    """The engine."""
    def __init__(self, left, right):
        leftState = StateDriver([]) # Would be an emitter.
        rightState = StateDriver([]) # Would be an emitter.
        # Add the state controller to the chain of controllers of the drivers.
        # Real driver might need API to work on chained controllers.
        self.left = StateController(left, leftState, rightState)
        self.right = StateController(right, rightState, leftState)

    def debug(self, title):
        print('\n')
        print(title)
        print("left:       %s"% self.left.driver.messages)
        print("state left: %s"% self.left.state.messages)
        print("rght:       %s"% self.right.driver.messages)
        print("state rght: %s"% self.right.state.messages)

    def run(self):
        leftMessages = self.left.getChanges() # Would be async.
        rightMessages = self.right.getChanges() # Would be async.

        print("## Changes:")
        print("- from left: %s"% leftMessages)
        print("- from rght: %s"% rightMessages)

        self.left.update(rightMessages)
        self.right.update(leftMessages)

        print("\n## Update done.")

if __name__ == '__main__':

    # Messages
    m1r = Message(1, "1 body")
    m1l = Message(1, "1 body") # Same as m1r.

    m2r = Message(2, "2 body")
    m2l = Message(2, "2 body") # Same as m2r.
    #TODO: first sync when one side is not empty or identical.
    # Supporting this feature is really challenging!
    # m2l.markRead()              # Same as m2r but read.

    m3r = Message(3, "3 body") # Not at left.

    #TODO: None UID is meant for new messages in Maildir.
    # m4l = Message(None, "4 body") # Not at right.

    leftMessages = Messages([m1l, m2l])
    rghtMessages = Messages([m1r, m2r, m3r])

    # Fill both sides with pre-existing data.
    left = Driver(leftMessages) # Fake those data.
    right = Driver(rghtMessages) # Fake those data.

    # Start engine.
    engine = Engine(left, right)
    engine.debug("Initial content of both sides:")

    print("\n# PASS 1")
    engine.run()
    engine.debug("# Run of PASS 1: done.")
    m2r.markImportant()
    engine.debug("# After Run1 but before Run 2")

    print("\n# PASS 2")
    engine.run()
    engine.debug("# Run of PASS 2: done.")

    #TODO: PASS 3 with changed messages.
