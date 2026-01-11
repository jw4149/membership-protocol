/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <algorithm>
#include <cstring>
#include <iterator>
#include <string>
#include <vector>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
void MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *msg = (MessageHdr *)data;
    if (msg->msgType == JOINREQ) {
        // Handle JOINREQ
        handleJoinReq(msg);
    } else if (msg->msgType == JOINREP) {
        // Handle JOINREP
        memberNode->inGroup = true;
        updateMembershipList(msg, size);
    } else if (msg->msgType == GOSSIP) {
        // Handle GOSSIP
        updateMembershipList(msg, size);
    }
}

void MP1Node::handleJoinReq(MessageHdr *msg) {
    Address addr;
    int id;
    short port;
    memcpy(addr.addr, (char *)(msg+1), sizeof(addr.addr));
    memcpy(&id, &addr.addr[0], sizeof(int));
    memcpy(&port, &addr.addr[4], sizeof(short));

    long heartbeat;
    memcpy(&heartbeat, (char *)(msg+1) + sizeof(addr.addr) + 1, sizeof(long));

    MemberListEntry entry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(entry);
    memberNode->nnb++;
    log->logNodeAdd(&memberNode->addr, &addr);

    sendMembershipList(&addr, JOINREP);
}

void MP1Node::updateMembershipList(MessageHdr *msg, int size) {
    int numEntries = (size - sizeof(MessageHdr)) / sizeof(MemberListEntry);
    MemberListEntry *entries = (MemberListEntry *)(msg + 1);
    for (int i = 0; i < numEntries; i++) {
        auto failedIt = find_if(failedList.begin(), failedList.end(), 
                            [&](MemberListEntry &entry){return entry.id == entries[i].id && entry.port == entries[i].port;});
        if (failedIt != failedList.end()) {
            // node already marked failed. discard this gossip entry
            continue;
        }
        auto it = find_if(memberNode->memberList.begin(), memberNode->memberList.end(), 
                    [&](MemberListEntry &entry){return entry.id == entries[i].id && entry.port == entries[i].port;});
        if (it != memberNode->memberList.end()) {
            // found
            if (entries[i].heartbeat > it->heartbeat) {
                it->heartbeat = entries[i].heartbeat;
                it->timestamp = par->getcurrtime();
            }
        } else {
            // not found
            MemberListEntry newEntry(entries[i].id, entries[i].port, entries[i].heartbeat, par->getcurrtime());
            memberNode->memberList.push_back(newEntry);
            memberNode->nnb++;
            Address addr(to_string(entries[i].id) + ":" + to_string(entries[i].port));
            log->logNodeAdd(&memberNode->addr, &addr);
        }
    }
}

void MP1Node::sendMembershipList(Address *toaddr, MsgTypes msgType) {
    size_t msgsize = sizeof(MessageHdr) + memberNode->memberList.size() * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = msgType;
    memcpy((char *)(msg+1), memberNode->memberList.data(), memberNode->memberList.size() * sizeof(MemberListEntry));
    emulNet->ENsend(&memberNode->addr, toaddr, (char *)msg, msgsize);
    free(msg);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    memberNode->heartbeat++;
    int id;
    short port;
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));

    auto it = find_if(memberNode->memberList.begin(), memberNode->memberList.end(), 
                [&id, &port](MemberListEntry &entry) {return entry.id == id && entry.port == port;});
    if (it != memberNode->memberList.end()) {
        it->heartbeat = memberNode->heartbeat;
        it->timestamp = par->getcurrtime();
    }
    
    removeFailedNodes();
    sendGossipMessages();
    return;
}

void MP1Node::removeFailedNodes() {
    auto it = memberNode->memberList.begin();
    while (it != memberNode->memberList.end()) {
        if (par->getcurrtime() - it->timestamp > TFAIL) {
            Address addr(to_string(it->id) + ":" + to_string(it->port));
            failedList.push_back(*it);
            it = memberNode->memberList.erase(it);
            memberNode->nnb--;
            log->logNodeRemove(&memberNode->addr, &addr);
        } else {
            ++it;
        }
    }
    failedList.erase(
        remove_if(failedList.begin(), failedList.end(), 
                        [&](MemberListEntry &entry){return par->getcurrtime() - entry.timestamp > TREMOVE;}),
        failedList.end()
    );
}

void MP1Node::sendGossipMessages() {
    for (const auto &entry : memberNode->memberList) {
        Address addr;
        memset(&addr, 0, sizeof(Address));
        *(int *)(&addr.addr[0]) = entry.id;
        *(short *)(&addr.addr[4]) = entry.port;
        sendMembershipList(&addr, GOSSIP);
    }
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    int id;
    short port;
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));
    MemberListEntry entry(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(entry);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
