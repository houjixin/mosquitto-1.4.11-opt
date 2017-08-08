/*
* author: 			jason.hou
* create date:		2017.03.07
* last modifiy date:	2017.03.07
* email:	 			houjixin@163.com
* qq:				278200053
* description: 本文主要描述epoll相关的函数，以及epoll操作配套的主流程函数
*/

#include "mosquitto_internal.h"
#include "tls_mosq.h"

#include <ctype.h>
#include <signal.h>
#include <errno.h>
#include <mosquitto_broker.h>
#include <memory_mosq.h>

#include <config.h>
#include <sys/epoll.h>
#include <sys/time.h>

#define EPOLL_READ 0x01
#define EPOLL_WRITE 0x02
#define INVALID_FD -1
#define EPOLL_CREATE_NUM 256
#define EPOLL_EVENTS_NUM_UNIT 1024
#define EPOLL_WAIT_TIME 100
#define EPOLL_EVENT_RESERVED_NUM 100

extern bool flag_tree_print;
extern bool flag_reload;
extern int run;
#ifdef WITH_SYS_TREE
extern int g_clients_expired;
#endif

static int g_epoll_fd = INVALID_FD;
static struct epoll_event* g_epoll_events = NULL;
static int g_max_epoll_events_num = 0;
static bool is_listensocket(int fd, int *listensock, int listensock_count);
#ifdef WITH_BROKER
static int epoll_add_read_write(struct mosquitto_db* db, int new_socket);
#endif
static int epoll_init(struct mosquitto_db* db, int listensock_count);
static int add_epoll_events_unit(struct mosquitto_db* db, int new_size);
static int epoll_add_read(struct mosquitto_db* db, int new_socket);
static int loop_handle_results_epoll(struct mosquitto_db *db, int events_num, int *listensock, int listensock_count);
static void check_persistent_client(struct mosquitto_db *db, time_t now_time, time_t expiration_check_time);
static void send_msg(struct mosquitto_db *db);
void send_notice_client_status(struct mosquitto_db *db, char* client_id, int type, char* reason);


#ifdef WITH_BROKER
static void check_keep_alive(struct mosquitto_db *db);
#else
static void check_keep_alive();
#endif

#ifdef WITH_BRIDGE
static void handle_bridge(struct mosquitto_db *db);
#endif



int mosquitto_main_loop_epoll(struct mosquitto_db *db, mosq_sock_t *listensock, int listensock_count, int listener_max)
{
#ifdef WITH_SYS_TREE
	time_t start_time = mosquitto_time();
#endif
#ifdef WITH_PERSISTENCE
	time_t last_backup = mosquitto_time();
#endif
	time_t now_time;
#ifndef WIN32
	sigset_t sigblock, origsig;
#endif
	int i;
	int eventcnt = 0;

	time_t last_timeout_check = 0;
	time_t expiration_check_time = 0;

#ifndef WIN32
	sigemptyset(&sigblock);
	sigaddset(&sigblock, SIGINT);
#endif
	_mosquitto_log_printf(NULL, MOSQ_LOG_INFO, "[mosquitto_main_loop_epoll]: epoll mode start!");

	if(MOSQ_ERR_SUCCESS != epoll_init(db, listensock_count)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[mosquitto_main_loop_epoll]: initialize epoll fail!");
		return MOSQ_ERR_INVAL;;
	}

	for(i=0; i<listensock_count; i++){
		if(!IS_VALID_FD(listensock[i]))
			continue;
		if(MOSQ_ERR_SUCCESS != epoll_add_read(db, listensock[i])){
			_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[mosquitto_main_loop_epoll]: Adding read event for listen socket fd = %d into epoll fail", listensock[i]);
			return MOSQ_ERR_INVAL;;
		}
	}
		
	if(db->config->persistent_client_expiration > 0){
		expiration_check_time = time(NULL) + 3600;
	}

	while(run){
		now_time = time(NULL);
		mosquitto__free_disused_contexts(db);
#ifdef WITH_SYS_TREE
		if(db->config->sys_interval > 0){
			mqtt3_db_sys_update(db, db->config->sys_interval, start_time);
		}
#endif

		check_keep_alive(db);
		//handle bridge if we use!
		if(db->config->with_bridge){
#ifdef WITH_BRIDGE
			handle_bridge(db);
#endif
		}
		
		check_persistent_client(db, now_time, expiration_check_time);

		if(last_timeout_check < mosquitto_time()){
			/* Only check at most once per second. */
			mqtt3_db_message_timeout_check(db, db->config->retry_interval);
			last_timeout_check = mosquitto_time();
		}

		send_msg(db);
		
#ifndef WIN32
		sigprocmask(SIG_SETMASK, &sigblock, &origsig);
		eventcnt = epoll_wait(g_epoll_fd, g_epoll_events, g_max_epoll_events_num, EPOLL_WAIT_TIME);
		sigprocmask(SIG_SETMASK, &origsig, NULL);
#endif

		if(eventcnt < 0){
			_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[mosquitto_main_loop_epoll] error happened in epoll! errorno:%d, error info:%s", errno, strerror(errno));
		}else{
			loop_handle_results_epoll(db, eventcnt, listensock, listensock_count);
		}

		if(eventcnt >= (g_max_epoll_events_num - EPOLL_EVENT_RESERVED_NUM)){
			if(MOSQ_ERR_SUCCESS != add_epoll_events_unit(db, EPOLL_EVENTS_NUM_UNIT)){
				_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[mosquitto_main_loop_epoll] out of memory! allocate memory for epoll event fail!");
				return MOSQ_ERR_INVAL;
			}
		}
#ifdef WITH_PERSISTENCE
		if(db->config->persistence && db->config->autosave_interval){
			if(db->config->autosave_on_changes){
				if(db->persistence_changes >= db->config->autosave_interval){
					mqtt3_db_backup(db, false);
					db->persistence_changes = 0;
				}
			}else{
				if(last_backup + db->config->autosave_interval < mosquitto_time()){
					mqtt3_db_backup(db, false);
					last_backup = mosquitto_time();
				}
			}
		}
#endif

#ifdef WITH_PERSISTENCE
		if(flag_db_backup){
			mqtt3_db_backup(db, false);
			flag_db_backup = false;
		}
#endif
		if(flag_reload){
			_mosquitto_log_printf(NULL, MOSQ_LOG_INFO, "Reloading config.");
			mqtt3_config_read(db->config, true);
			mosquitto_security_cleanup(db, true);
			mosquitto_security_init(db, true);
			mosquitto_security_apply(db);
			mqtt3_log_close(db->config);
			mqtt3_log_init(db->config);
			flag_reload = false;
		}
		if(flag_tree_print){
			mqtt3_sub_tree_print(&db->subs, 0);
			flag_tree_print = false;
		}
#ifdef WITH_WEBSOCKETS
		for(i=0; i<db->config->listener_count; i++){
			/* Extremely hacky, should be using the lws provided external poll
			 * interface, but their interface has changed recently and ours
			 * will soon, so for now websockets clients are second class
			 * citizens. */
			if(db->config->listeners[i].ws_context){
				libwebsocket_service(db->config->listeners[i].ws_context, 0);
			}
		}
		if(db->config->have_websockets_listener){
			temp__expire_websockets_clients(db);
		}
#endif
	}

	if(g_epoll_events){
		_mosquitto_free(g_epoll_events);
		g_epoll_events = NULL;
	}
	return MOSQ_ERR_SUCCESS;
}

int epoll_init(struct mosquitto_db* db, int listensock_count)
{
	if(!IS_VALID_FD(g_epoll_fd)){
		g_epoll_fd = epoll_create(EPOLL_CREATE_NUM);
	}
	return add_epoll_events_unit(db, (listensock_count + EPOLL_EVENTS_NUM_UNIT));
}

/*
* increase epoll events array size
*/
int add_epoll_events_unit(struct mosquitto_db* db, int new_size)
{
	if(new_size <= 0)
		return MOSQ_ERR_INVAL;
	g_max_epoll_events_num = new_size + g_max_epoll_events_num;
	if(!IS_VALID_POINTER(g_epoll_events)){		
		g_epoll_events = _mosquitto_malloc(sizeof(struct epoll_event) * g_max_epoll_events_num);
	}
	else{
		g_epoll_events = _mosquitto_realloc(g_epoll_events, sizeof(struct epoll_event) * g_max_epoll_events_num);
	}
	if(!g_epoll_events){
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "[add_epoll_events_unit] out of memory. for struct epoll_event fail!");
		return MOSQ_ERR_NOMEM;
	}
	return MOSQ_ERR_SUCCESS;
}


/*
*function name:epoll_add_read
* last modify date: 2017.03.07
* description: Only add  read event for new socket into epoll
*/
int epoll_add_read(struct mosquitto_db* db, int new_socket)
{
	if(!db || !IS_VALID_FD(g_epoll_fd) || !IS_VALID_FD(new_socket))
		return MOSQ_ERR_INVAL;
	struct epoll_event ee;
	ee.data.fd = new_socket;
	ee.events = (EPOLLIN | EPOLLERR | EPOLLHUP);
	
	if(epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, new_socket, &ee)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[epoll_add_read] :add read event for socket(fd = %d) into epoll fail!", new_socket);
		return MOSQ_ERR_INVAL;
	}
	else		
		return MOSQ_ERR_SUCCESS;				
}

/*
*function name:epoll_add_read
* last modify date: 2017.03.07
* description: We are interested in reading event for all sockets, 
* but only interested in writting event for the socket wich have message to send.
*
*/
int epoll_remove_write(struct mosquitto_db* db, int new_socket)
{
	if(!db || !IS_VALID_FD(g_epoll_fd) || !IS_VALID_FD(new_socket))
		return MOSQ_ERR_INVAL;
	struct epoll_event ee;
	ee.data.fd = new_socket;
	ee.events = (EPOLLIN | EPOLLERR | EPOLLHUP);
	//we also need reading event
	if(epoll_ctl(g_epoll_fd, EPOLL_CTL_MOD, new_socket, &ee)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[epoll_remove_write]: remove write event for socket(fd = %d) into epoll fail!", new_socket);
		return MOSQ_ERR_INVAL;
	}
	else		
		return MOSQ_ERR_SUCCESS;				
}

#ifdef WITH_BRIDGE
int epoll_add_read_write(struct mosquitto_db* db, int new_socket)
{
	if(!db || !IS_VALID_FD(g_epoll_fd) || !IS_VALID_FD(new_socket))
		return MOSQ_ERR_INVAL;
	struct epoll_event ee;
	ee.data.fd = new_socket;
	ee.events = (EPOLLOUT | EPOLLIN  | EPOLLERR | EPOLLHUP);
	
	if(epoll_ctl(g_epoll_fd, EPOLL_CTL_MOD, new_socket, &ee)){
		_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[epoll_add_read] :add read event for socket(fd = %d) into epoll fail!", new_socket);
		return MOSQ_ERR_INVAL;
	}
	else		
		return MOSQ_ERR_SUCCESS;				
}
#endif


int loop_handle_results_epoll(struct mosquitto_db *db, int events_num, int *listensock, int listensock_count)
{
	int err;
	socklen_t len;
	int i;
	uint32_t events = 0;
	int new_con_num = 0;
	int read_event_num = 0;
	int write_event_num = 0;
	int read_oper_num = 0;
	int write_oper_num = 0;
	int error_event_num = 0;
	int res = 0;
	int new_business_socket = -1;
	if(events_num <= 0)
		return MOSQ_ERR_SUCCESS;
	for(i = 0; i < events_num; i++){
		int sock = g_epoll_events[i].data.fd;
		events = g_epoll_events[i].events;

		// Handle listen socket
		if((events & EPOLLIN) && is_listensocket(sock, listensock, listensock_count)){
			do{
				++new_con_num;
				new_business_socket = mqtt3_socket_accept(db, sock); 
				if(!IS_VALID_FD(new_business_socket)) break;				
				epoll_add_read(db, new_business_socket);
			}while(true);
			continue;	
		}

		//Find context for current ready socket!
		struct mosquitto* context = NULL;
		HASH_FIND(hh_sock, db->contexts_by_sock, &sock, sizeof(mosq_sock_t), context);
		if(!IS_VALID_POINTER(context)){
			_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[loop_handle_results_epoll]: Cann't find the context with socket(%d)!", sock);
			continue;
		}	
		
		// Handle read event 	
		if (events & EPOLLIN){	
			++read_event_num;
			//Handle read event for business socket
#ifdef WITH_TLS//if use tls we need to judge more conditions
			if(context->ssl && context->state == mosq_cs_new){
#endif
				do{
					do{//there will be more than one packet coming in.
						++read_oper_num;
						res = _mosquitto_packet_read(db, context);
						_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[loop_handle_results_epoll] : read data for context(%s) over! res:%d", context->id, res);
						if(MOSQ_ERR_SUCCESS != res){
							if(MOSQ_ERR_SOCKET_EAGAIN == res){
								/*we have read all data for current context!
								* but the socket of this context is OK.
								*/
								break;
							}
							//error happened!						
							send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "read packet fail!");
							do_disconnect(db, context);							
						}
					}while(MOSQ_ERR_SUCCESS == res);

					if((MOSQ_ERR_SUCCESS != res) && (MOSQ_ERR_SOCKET_EAGAIN != res)){
						continue;
					}
				}while(SSL_DATA_PENDING(context));
#ifdef WITH_TLS//if use tls we need more condition
			}
#endif
		}

		//Handle write event
		if(events & EPOLLOUT){
			++write_event_num;
#ifdef WITH_TLS//if use tls we need to judge more conditions
			if(context->want_write || (context->ssl && context->state == mosq_cs_new)){
#endif
				if(context->state == mosq_cs_connect_pending){
					len = sizeof(int);
					if(!getsockopt(context->sock, SOL_SOCKET, SO_ERROR, (char *)&err, &len)){
						if(err == 0){
							context->state = mosq_cs_new;
						}
					}else{				
						send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "get socket option fail!");
						do_disconnect(db, context);
						continue;
					}
				}
				do{//there will be more than one packet to write.
					++write_oper_num;
					res = _mosquitto_packet_write(db, context);
					_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[loop_handle_results_epoll] : write data for context(%s) over!", context->id);
					if(MOSQ_ERR_SUCCESS != res){
						if(MOSQ_ERR_SOCKET_EAGAIN == res){
							/*we socket of current context is full, but we alse have data to write,
							* at is sitution, the socket is OK.
							*/
							break;
						}
						//error happened!						
						send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "write packet fail!");
						do_disconnect(db, context);
						continue;
					}
				}while(MOSQ_ERR_SUCCESS == res);
#ifdef WITH_TLS
			}
#endif
			//remove write event for write socket, because we have handled it's write event
			if(MOSQ_ERR_SUCCESS != epoll_remove_write(db, sock)){
				_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[loop_handle_results_epoll] :add read event for socket(fd = %d) into epoll fail!", sock);
				continue;
			}
		}
			//Handle error and hup event
		if(events & (EPOLLERR | EPOLLHUP)){
			_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[loop_handle_results_epoll] EPOLLERR | EPOLLHUP from current socket! client id:%s\n", context->id);
			send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "Read EPOLLERR | EPOLLHUP from current socket");
			do_disconnect(db, context);
			continue;
		}
	}
	
	_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[loop_handle_results_epoll] event number: total(%d); read event:(%d) oper:(%d); newcon(%d); write event:(%d) oper:(%d); error(%d);\n", events_num, read_event_num, read_oper_num, new_con_num, write_event_num, write_oper_num, error_event_num);
	return MOSQ_ERR_SUCCESS;
}

bool is_listensocket(int fd, int *listensock, int listensock_count)
{
	if(!IS_VALID_POINTER(listensock))
		return false;
	bool bRes = false;
	int i=0;
	for(i=0; i<listensock_count; ++i)
	{
		if(fd == listensock[i])
		{
			bRes = true;
			break;
		}
	}
	return bRes;
}
#ifdef WITH_BROKER
static void check_keep_alive(struct mosquitto_db *db)

#else
static void check_keep_alive()
#endif
{
	struct mosquitto *context, *ctxt_tmp;
	static time_t last_check = 0;
	time_t now = mosquitto_time();

#ifdef WITH_BRIDGE
		mosq_sock_t bridge_sock;
#endif
	
	if(now - last_check < 60) return;
	
	HASH_ITER(hh_id, db->contexts_by_id, context, ctxt_tmp){
		if(context == NULL)
			continue;
#ifdef WITH_BRIDGE
		if(context->bridge){
			_mosquitto_check_keepalive(db, context);
			if(context->bridge->round_robin == false
				&& context->bridge->cur_address != 0
				&& now > context->bridge->primary_retry){
				if(_mosquitto_try_connect(context, context->bridge->addresses[0].address, context->bridge->addresses[0].port, &bridge_sock, NULL, false) <= 0){
					COMPAT_CLOSE(bridge_sock);
					_mosquitto_socket_close(db, context);
					context->bridge->cur_address = context->bridge->address_count-1;
				}
			}
		}
#endif
		/* Local bridges never time out in this fashion. */
		if(!(context->keepalive)|| context->bridge
			|| now - context->last_msg_in < (time_t)(context->keepalive)*3/2){
			continue;
		}else{
			send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "over limitation of keepalive * 1.5");					
			do_disconnect(db, context);		
		}
	}
	last_check = mosquitto_time();
	
}


#ifdef WITH_BRIDGE
static void handle_bridge(struct mosquitto_db *db)
{
	time_t now_time;
	int time_count;
	time_t now = 0;

	mosq_sock_t bridge_sock;
	int rc;

	now_time = time(NULL);
	
	time_count = 0;
	HASH_ITER(hh_sock, db->contexts_by_sock, context, ctxt_tmp){
		if(time_count > 0){
			time_count--;
		}else{
			time_count = 1000;
			now = mosquitto_time();
		}
		context->pollfd_index = -1;
	
		if(context->sock != INVALID_SOCKET){
#ifdef WITH_BRIDGE
			if(context->bridge){
				_mosquitto_check_keepalive(db, context);
				if(context->bridge->round_robin == false
					&& context->bridge->cur_address != 0
					&& now > context->bridge->primary_retry){
	
					if(_mosquitto_try_connect(context, context->bridge->addresses[0].address, context->bridge->addresses[0].port, &bridge_sock, NULL, false) <= 0){
						COMPAT_CLOSE(bridge_sock);
						_mosquitto_socket_close(db, context);
						context->bridge->cur_address = context->bridge->address_count-1;
					}
				}
		}
#endif
	
		/* Local bridges never time out in this fashion. */
		if(!(context->keepalive)
			|| context->bridge
			|| now - context->last_msg_in < (time_t)(context->keepalive)*3/2){
	
			if(context->current_out_packet || context->state == mosq_cs_connect_pending){
				epoll_add_read_write(db, context->sock);
			}
	
	
			/*if(mqtt3_db_message_write(db, context) == MOSQ_ERR_SUCCESS){
					if(context->current_out_packet || context->state == mosq_cs_connect_pending){
						epoll_add_read_write(db, context->sock);
					}
				}else{
					send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "write message fail!");
					do_disconnect(db, context);
				}*/
		}else{
			if(db->config->connection_messages == true){
				if(context->id){
					id = context->id;
				}else{
					id = "<unknown>";
				}
				_mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s has exceeded timeout, disconnecting.", id);
			}
			/* Client has exceeded keepalive*1.5 */
			send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "Client has exceeded timeout, disconnecting");			
			do_disconnect(db, context);
		}
	}
}
	
#ifdef WITH_BRIDGE
	time_count = 0;
	for(i=0; i<db->bridge_count; i++){
		if(!db->bridges[i]) continue;
	
			context = db->bridges[i];
	
			if(context->sock == INVALID_SOCKET){
				if(time_count > 0){
					time_count--;
				}else{
					time_count = 1000;
					now = mosquitto_time();
				}
				/* Want to try to restart the bridge connection */
				if(!context->bridge->restart_t){
					context->bridge->restart_t = now+context->bridge->restart_timeout;
					context->bridge->cur_address++;
					if(context->bridge->cur_address == context->bridge->address_count){
						context->bridge->cur_address = 0;
					}
					if(context->bridge->round_robin == false && context->bridge->cur_address != 0){
						context->bridge->primary_retry = now + 5;
					}
				}else{
					if((context->bridge->start_type == bst_lazy && context->bridge->lazy_reconnect)
						|| (context->bridge->start_type == bst_automatic && now > context->bridge->restart_t)){
	
#if defined(__GLIBC__) && defined(WITH_ADNS)
						if(context->adns){
						/* Waiting on DNS lookup */
							rc = gai_error(context->adns);
							if(rc == EAI_INPROGRESS){
								/* Just keep on waiting */
							}else if(rc == 0){
								rc = mqtt3_bridge_connect_step2(db, context);
								if(rc == MOSQ_ERR_SUCCESS){
									if(context->current_out_packet){
										epoll_add_read_write(db, context->sock);
									}
								}else{
									context->bridge->cur_address++;
									if(context->bridge->cur_address == context->bridge->address_count){
										context->bridge->cur_address = 0;
									}
								}
							}else{
								/* Need to retry */
								if(context->adns->ar_result){
									freeaddrinfo(context->adns->ar_result);
								}
								_mosquitto_free(context->adns);
								context->adns = NULL;
							}
						}else{
							rc = mqtt3_bridge_connect_step1(db, context);
							if(rc){
								context->bridge->cur_address++;
								if(context->bridge->cur_address == context->bridge->address_count){
									context->bridge->cur_address = 0;
								}
							}
						}
#else
						{
							rc = mqtt3_bridge_connect(db, context);
							if(rc == MOSQ_ERR_SUCCESS){
								if(context->current_out_packet){
									epoll_add_read_write(db, context->sock);
								}
							}else{
								context->bridge->cur_address++;
								if(context->bridge->cur_address == context->bridge->address_count){
									context->bridge->cur_address = 0;
								}
							}
						}
#endif
					}
				}
			}
		}
#endif

}
#endif

static void check_persistent_client(struct mosquitto_db *db, time_t now_time, time_t expiration_check_time)
{
	struct mosquitto *context, *ctxt_tmp;
	char *id;
	if(db->config->persistent_client_expiration > 0 && now_time > expiration_check_time){
		HASH_ITER(hh_id, db->contexts_by_id, context, ctxt_tmp){
			if(context->sock == INVALID_SOCKET && context->clean_session == 0){
				/* This is a persistent client, check to see if the
				 * last time it connected was longer than
				 * persistent_client_expiration seconds ago. If so,
				 * expire it and clean up.
				 */
				if(now_time > context->disconnect_t+db->config->persistent_client_expiration){
					if(context->id){
						id = context->id;
					}else{
						id = "<unknown>";
					}
					_mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "[check_persistent_client]Expiring persistent client %s due to timeout.", id);
#ifdef WITH_SYS_TREE
					g_clients_expired++;
#endif
					context->clean_session = true;
					context->state = mosq_cs_expiring;					
					send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "Expiring persistent client due to timeout");
					do_disconnect(db, context);
				}
			}
		}
		expiration_check_time = time(NULL) + 3600;
	}

}

static void send_msg(struct mosquitto_db *db)
{
	int context_count = 0;
	struct mosquitto *context, *ctxt_tmp;
	context_count = HASH_CNT(hh_msg_sock, db->contexts_with_msg_by_sock);
	if(context_count <= 0)
		return;
	_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[send_msg] context_count:%d", context_count);

	HASH_ITER(hh_msg_sock, db->contexts_with_msg_by_sock, context, ctxt_tmp){
		if(mqtt3_db_message_write(db, context) != MOSQ_ERR_SUCCESS){			
			send_notice_client_status(db, context->id, ONTICE_TYPE_OFFLINE, "write message fail!");
			do_disconnect(db, context);
		}else if(!IS_VALID_POINTER(context->msgs)){
			HASH_DELETE(hh_msg_sock, db->contexts_with_msg_by_sock, context);
		}
	}
}

/*
* type == ONTICE_TYPE_ONLINE or ONTICE_TYPE_OFFLINE
*不能再do_disconnect函数里面调用该函数，以踢掉某个在线客户端为例
*假如ID为A的客户端已经在线了，这时如果A再次上线，那么在connect
*的时候就会发布一个 A上线的通知,但是在检测ID A时就会把原来的给
*踢下去，这时就会发一个A下线的通知，对于接收通知的来说最终ID
*为A的连接断开了，但是实际在mosquitto里A还在线上；
*/
void send_notice_client_status(struct mosquitto_db *db, char* client_id, int type, char* reason)
{
	if(!IS_VALID_POINTER(client_id)) return;
	
	char buf[1024] = {0};
	struct	timeval cur_time;
	long int timestamp = 0;
	gettimeofday(&cur_time,NULL);
	struct mosquitto_msg_store *stored;
	
	timestamp = (cur_time.tv_sec*1000000+cur_time.tv_usec) / 1000;
	if(IS_VALID_POINTER(reason)){
		snprintf(buf, 1024, "{\"clientid\":\"%s\",\"type\":\"%d\",\"time\":%ld, \"reason\":\"%s\"}", client_id, type, timestamp, reason);
	}else{
		snprintf(buf, 1024, "{\"clientid\":\"%s\",\"type\":\"%d\",\"time\":%ld}", client_id, type, timestamp);
	}
	
	_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "[send_notice_client_status] msg:%s", buf);
	if(ONTICE_TYPE_ONLINE == type && IS_VALID_POINTER(db->config->topic_notice_online)){
		if(mqtt3_db_message_store(db, db->pid, 0, db->config->topic_notice_online, 0, strlen(buf), buf, 0, &stored, 0)) return ;
		mqtt3_db_messages_queue(db, db->pid, db->config->topic_notice_online, 0, 0, &stored);
	}else if(ONTICE_TYPE_OFFLINE == type && IS_VALID_POINTER(db->config->topic_notice_offline)){
		if(mqtt3_db_message_store(db, db->pid, 0, db->config->topic_notice_offline, 0, strlen(buf), buf, 0, &stored, 0)) return ;
		mqtt3_db_messages_queue(db, db->pid, db->config->topic_notice_offline, 0, 0, &stored);
	}
}



