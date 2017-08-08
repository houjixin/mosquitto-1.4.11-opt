/*
Copyright (c) 2009-2014 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Eclipse Distribution License v1.0 which accompany this distribution.
 
The Eclipse Public License is available at
   http://www.eclipse.org/legal/epl-v10.html
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.
 
Contributors:
   Roger Light - initial implementation and documentation.
*/

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include <config.h>

#include <mosquitto_broker.h>
#include <mqtt3_protocol.h>
#include <memory_mosq.h>
#include <read_handle.h>
#include <send_mosq.h>
#include <util_mosq.h>

#include <stdio.h>
#include <sys/stat.h> 
#include <sys/types.h>
#include <time.h>

#define TMP_MAX_BUFFER 4096
#define MAX_FILE_NAME 1024
#define TIME_BUF_LEN 512

static time_t g_last_dump_time;

static int dump_online_users(struct mosquitto_db *db);

#ifdef WITH_SYS_TREE
extern uint64_t g_pub_bytes_received;
#endif

int mqtt3_packet_handle(struct mosquitto_db *db, struct mosquitto *context)
{
	if(!context) return MOSQ_ERR_INVAL;

	switch((context->in_packet.command)&0xF0){
		case PINGREQ:
			return _mosquitto_handle_pingreq(context);
		case PINGRESP:
			return _mosquitto_handle_pingresp(context);
		case PUBACK:
			return _mosquitto_handle_pubackcomp(db, context, "PUBACK");
		case PUBCOMP:
			return _mosquitto_handle_pubackcomp(db, context, "PUBCOMP");
		case PUBLISH:
			return mqtt3_handle_publish(db, context);
		case PUBREC:
			return _mosquitto_handle_pubrec(context);
		case PUBREL:
			return _mosquitto_handle_pubrel(db, context);
		case CONNECT:
			return mqtt3_handle_connect(db, context);
		case DISCONNECT:
			return mqtt3_handle_disconnect(db, context);
		case SUBSCRIBE:
			return mqtt3_handle_subscribe(db, context);
		case UNSUBSCRIBE:
			return mqtt3_handle_unsubscribe(db, context);
#ifdef WITH_BRIDGE
		case CONNACK:
			return mqtt3_handle_connack(db, context);
		case SUBACK:
			return _mosquitto_handle_suback(context);
		case UNSUBACK:
			return _mosquitto_handle_unsuback(context);
#endif
		default:
			/* If we don't recognise the command, return an error straight away. */
			return MOSQ_ERR_PROTOCOL;
	}
}

int mqtt3_handle_publish(struct mosquitto_db *db, struct mosquitto *context)
{
	char *topic;
	void *payload = NULL;
	uint32_t payloadlen;
	uint8_t dup, qos, retain;
	uint16_t mid = 0;
	int rc = 0;
	uint8_t header = context->in_packet.command;
	int res = 0;
	struct mosquitto_msg_store *stored = NULL;
	int len;
	char *topic_mount;
#ifdef WITH_BRIDGE
	char *topic_temp;
	int i;
	struct _mqtt3_bridge_topic *cur_topic;
	bool match;
#endif

	dup = (header & 0x08)>>3;
	qos = (header & 0x06)>>1;
	if(qos == 3){
		_mosquitto_log_printf(NULL, MOSQ_LOG_INFO,
				"Invalid QoS in PUBLISH from %s, disconnecting.", context->id);
		return 1;
	}
	retain = (header & 0x01);

	if(_mosquitto_read_string(&context->in_packet, &topic)) return 1;
	if(STREMPTY(topic)){
		/* Invalid publish topic, disconnect client. */
		_mosquitto_free(topic);
		return 1;
	}
#ifdef WITH_BRIDGE
	if(context->bridge && context->bridge->topics && context->bridge->topic_remapping){
		for(i=0; i<context->bridge->topic_count; i++){
			cur_topic = &context->bridge->topics[i];
			if((cur_topic->direction == bd_both || cur_topic->direction == bd_in) 
					&& (cur_topic->remote_prefix || cur_topic->local_prefix)){

				/* Topic mapping required on this topic if the message matches */

				rc = mosquitto_topic_matches_sub(cur_topic->remote_topic, topic, &match);
				if(rc){
					_mosquitto_free(topic);
					return rc;
				}
				if(match){
					if(cur_topic->remote_prefix){
						/* This prefix needs removing. */
						if(!strncmp(cur_topic->remote_prefix, topic, strlen(cur_topic->remote_prefix))){
							topic_temp = _mosquitto_strdup(topic+strlen(cur_topic->remote_prefix));
							if(!topic_temp){
								_mosquitto_free(topic);
								return MOSQ_ERR_NOMEM;
							}
							_mosquitto_free(topic);
							topic = topic_temp;
						}
					}

					if(cur_topic->local_prefix){
						/* This prefix needs adding. */
						len = strlen(topic) + strlen(cur_topic->local_prefix)+1;
						topic_temp = _mosquitto_malloc(len+1);
						if(!topic_temp){
							_mosquitto_free(topic);
							return MOSQ_ERR_NOMEM;
						}
						snprintf(topic_temp, len, "%s%s", cur_topic->local_prefix, topic);
						topic_temp[len] = '\0';

						_mosquitto_free(topic);
						topic = topic_temp;
					}
					break;
				}
			}
		}
	}
#endif
	if(mosquitto_pub_topic_check(topic) != MOSQ_ERR_SUCCESS){
		/* Invalid publish topic, just swallow it. */
		_mosquitto_free(topic);
		return 1;
	}

	if(qos > 0){
		if(_mosquitto_read_uint16(&context->in_packet, &mid)){
			_mosquitto_free(topic);
			return 1;
		}
	}

	payloadlen = context->in_packet.remaining_length - context->in_packet.pos;
#ifdef WITH_SYS_TREE
	g_pub_bytes_received += payloadlen;
#endif
	if(context->listener && context->listener->mount_point){
		len = strlen(context->listener->mount_point) + strlen(topic) + 1;
		topic_mount = _mosquitto_malloc(len+1);
		if(!topic_mount){
			_mosquitto_free(topic);
			return MOSQ_ERR_NOMEM;
		}
		snprintf(topic_mount, len, "%s%s", context->listener->mount_point, topic);
		topic_mount[len] = '\0';

		_mosquitto_free(topic);
		topic = topic_mount;
	}

	if(payloadlen){
		if(db->config->message_size_limit && payloadlen > db->config->message_size_limit){
			_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "Dropped too large PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, dup, qos, retain, mid, topic, (long)payloadlen);
			goto process_bad_message;
		}
		payload = _mosquitto_calloc(payloadlen+1, 1);
		if(!payload){
			_mosquitto_free(topic);
			return 1;
		}
		if(_mosquitto_read_bytes(&context->in_packet, payload, payloadlen)){
			_mosquitto_free(topic);
			_mosquitto_free(payload);
			return 1;
		}
	}

	if(db->config->topic_change_keepalive && !strcmp(topic, db->config->topic_change_keepalive)){
		//change keep alive of current context
		//by jason.hou 2016.01.22
		int new_keepalive = atoi(payload);
		if(new_keepalive > 0){
			context->keepalive = new_keepalive;
			_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[mqtt3_handle_publish] topic %s ;client:%s, set keepalive: %d", topic, context->id, new_keepalive);
			return MOSQ_ERR_SUCCESS;
		}
		else{
			_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[mqtt3_handle_publish] topic %s ;client:%s, cann't get keepalive value from %s", topic, context->id, (char*)payload);
			return MOSQ_ERR_NOT_SUPPORTED;
		}
	}

	if(db->config->topic_dump_conn_info && db->config->cmd_dump_conn){
		if(!strcmp(db->config->topic_dump_conn_info, topic) && !strcmp(db->config->cmd_dump_conn, payload)){
			dump_online_users(db);
			return MOSQ_ERR_SUCCESS;
		}
	}

	/* Check for topic access */
	rc = mosquitto_acl_check(db, context, topic, MOSQ_ACL_WRITE);
	if(rc == MOSQ_ERR_ACL_DENIED){
		_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "Denied PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, dup, qos, retain, mid, topic, (long)payloadlen);
		goto process_bad_message;
	}else if(rc != MOSQ_ERR_SUCCESS){
		_mosquitto_free(topic);
		if(payload) _mosquitto_free(payload);
		return rc;
	}

	_mosquitto_log_printf(NULL, MOSQ_LOG_DEBUG, "Received PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, dup, qos, retain, mid, topic, (long)payloadlen);
	if(qos > 0){
		mqtt3_db_message_store_find(context, mid, &stored);
	}
	if(!stored){
		dup = 0;
		if(mqtt3_db_message_store(db, context->id, mid, topic, qos, payloadlen, payload, retain, &stored, 0)){
			_mosquitto_free(topic);
			if(payload) _mosquitto_free(payload);
			return 1;
		}
	}else{
		dup = 1;
	}
	switch(qos){
		case 0:
			if(mqtt3_db_messages_queue(db, context->id, topic, qos, retain, &stored)) rc = 1;
			break;
		case 1:
			if(mqtt3_db_messages_queue(db, context->id, topic, qos, retain, &stored)) rc = 1;
			if(_mosquitto_send_puback(context, mid)) rc = 1;
			break;
		case 2:
			if(!dup){
				res = mqtt3_db_message_insert(db, context, mid, mosq_md_in, qos, retain, stored);
			}else{
				res = 0;
			}
			/* mqtt3_db_message_insert() returns 2 to indicate dropped message
			 * due to queue. This isn't an error so don't disconnect them. */
			if(!res){
				if(_mosquitto_send_pubrec(context, mid)) rc = 1;
			}else if(res == 1){
				rc = 1;
			}
			break;
	}
	_mosquitto_free(topic);
	if(payload) _mosquitto_free(payload);

	return rc;
process_bad_message:
	_mosquitto_free(topic);
	if(payload) _mosquitto_free(payload);
	switch(qos){
		case 0:
			return MOSQ_ERR_SUCCESS;
		case 1:
			return _mosquitto_send_puback(context, mid);
		case 2:
			mqtt3_db_message_store_find(context, mid, &stored);
			if(!stored){
				if(mqtt3_db_message_store(db, context->id, mid, NULL, qos, 0, NULL, false, &stored, 0)){
					return 1;
				}
				res = mqtt3_db_message_insert(db, context, mid, mosq_md_in, qos, false, stored);
			}else{
				res = 0;
			}
			if(!res){
				res = _mosquitto_send_pubrec(context, mid);
			}
			return res;
	}
	return 1;
}


/*
* dump online user id to file
* return online file name
*/
int dump_online_users(struct mosquitto_db *db)
{
	char szFileName[MAX_FILE_NAME]  = {0};
	char szNowTimeStr[TIME_BUF_LEN] = {0};
	char online_buffer[TMP_MAX_BUFFER] = {0};
	time_t t_now;
	struct tm *s_tm_now = NULL;
	FILE *fd = 0;
	int  pos = 0;
	int  online_num = 0;
	int  wroted_num = 0;
	int  ret = 0;
	int copy_len = 0;
	struct mosquitto *context, *ctxt_tmp;

	time(&t_now);
	if(t_now-g_last_dump_time < 60){
		_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[dump_online_users] more than one command received in one minutes, default Once a minute.");
		return MOSQ_ERR_INVAL;
	}
	g_last_dump_time = t_now;
	/*s_tm_now is allocated by OS, and there is no need to releas it's memory*/
	s_tm_now = localtime(&t_now);
	if(s_tm_now == NULL)
	{
		_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[dump_online_users] get local time fail!");
		return MOSQ_ERR_INVAL;
	}
	memset(szNowTimeStr, 0, TIME_BUF_LEN);
	sprintf(szNowTimeStr,"%d-%d-%d-%d-%d-%d", 1900 + s_tm_now->tm_year, 1 + s_tm_now->tm_mon, s_tm_now->tm_mday, s_tm_now->tm_hour, s_tm_now->tm_min, s_tm_now->tm_sec);
	
	snprintf(szFileName, MAX_FILE_NAME-1, "/tmp/online_users/online_users_%d_%s.txt", db->config->listeners->port, szNowTimeStr);

	if(access("/tmp/online_users", 0) == -1)
	{
		if(mkdir("/tmp/online_users", 0777))
		{
			_mosquitto_log_printf(NULL, MOSQ_LOG_WARNING, "[dump_online_users] creat file bag failde.");
			return MOSQ_ERR_INVAL; 
		}
	}
	
	fd = fopen(szFileName, "a+");
	if(fd)
	{
		HASH_ITER(hh_sock, db->contexts_by_sock, context, ctxt_tmp){
			if(context->sock != INVALID_SOCKET)
			{
				/*copy id*/
				copy_len = strlen("id: ");
				memcpy(online_buffer+pos, "id: ", copy_len);
				pos += copy_len;

				copy_len = strlen(context->id);
		     	memcpy(online_buffer+pos, context->id, copy_len);
		        pos += copy_len;
				/*copy host*/
				copy_len = strlen(", address: ");
				memcpy(online_buffer+pos, ", address: ", copy_len);
				pos += copy_len;
				
				copy_len = strlen(context->address);
				memcpy(online_buffer+pos, context->address, copy_len);
				pos += copy_len;
				
				/*copy \n*/
		        memcpy(online_buffer+pos, "\n", strlen("\n"));
		        pos += 1;
		        online_num++;
	            if(pos+200 > TMP_MAX_BUFFER)
				{
					ret = fwrite(online_buffer, 1, pos, fd);
					if(ret < 0) break;
					fflush(fd);
					wroted_num += pos;
					pos = 0;
					memset(online_buffer, 0, TMP_MAX_BUFFER);
		        }
		    }
		}
		if((online_num > 0) && (wroted_num < online_num))
		{
			ret = fwrite(online_buffer, 1, pos, fd);
			if(ret < 0) goto end_dump;
			fflush(fd);
		}
		fprintf(fd, "Total number is :%d\n", online_num);
		fflush(fd);
		
		_mosquitto_log_printf(NULL, MOSQ_LOG_INFO, "[dump_online_users] suc Online Number:%d, dump file:%s\n", online_num, szFileName);
	}

end_dump:
	if(fd)
	{
		fclose(fd);
	}
	
	return MOSQ_ERR_SUCCESS;
}


