﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TequaCreek.BloxGuardianMessageProcessingService
{
    public class ApplicationValues
    {

        public static string MIME_TYPE_JSON = "application/json";

        public static int PARAMETER_STYLE_CLASSIC = 1;
        public static int PARAMETER_STYLE_REST_URL = 2;


        //////////////////////////////////
        // Query result column offsets ///
        //////////////////////////////////

        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID = 0;
        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID = 1;
        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_ORIGINATION_TYPE_CODE = 2;
        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD = 3;
        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID = 4;
        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID = 5;
        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID = 6;
        public const int MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID = 7;

        public const int INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_INTERNAL_ID = 0;
        public const int INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID = 1;
        public const int INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME = 2;
        public const int INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME = 3;
        public const int INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS = 4;

        public const int ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID = 0;
        public const int ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID = 1;
        public const int ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME = 2;
        public const int ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME = 3;
        public const int ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID = 4;
        public const int ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ALLOW_SHARE_LOCATION_INFO = 5;
        public const int ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ALLOW_PAYMENT_TXN_REQUEST = 6;

        public const int PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_TO_BLOXGUARDIAN_EXTERNAL_ID = 0;
        public const int PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_PUSHED_DATE_TIME = 1;
        public const int PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD = 2;

        public const int BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_EXTERNAL_ID = 0;
        public const int BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME = 1;
        public const int BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME = 2;
        public const int BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_EMAIL_ADDRESS = 3;

    }
}
