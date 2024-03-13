#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import base64
import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Set

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from cached_property import cached_property
from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount as FBAdAccount
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.user import User

from .base_insight_streams import AdsInsights
from .base_streams import FBMarketingIncrementalStream, FBMarketingReversedIncrementalStream, FBMarketingStream

logger = logging.getLogger("airbyte")


def fetch_thumbnail_data_url(url: str) -> Optional[str]:
    """Request thumbnail image and return it embedded into the data-link"""
    try:
        response = requests.get(url)
        if response.status_code == requests.status_codes.codes.OK:
            _type = response.headers["content-type"]
            data = base64.b64encode(response.content)
            return f"data:{_type};base64,{data.decode('ascii')}"
        else:
            logger.warning(f"Got {repr(response)} while requesting thumbnail image.")
    except requests.exceptions.RequestException as exc:
        logger.warning(f"Got {str(exc)} while requesting thumbnail image.")
    return None


class AdCreatives(FBMarketingStream):
    """AdCreative is append only stream
    doc: https://developers.facebook.com/docs/marketing-api/reference/ad-creative
    """

    entity_prefix = "adcreative"
    enable_deleted = False

    def __init__(self, fetch_thumbnail_images: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._fetch_thumbnail_images = fetch_thumbnail_images

    @cached_property
    def fields(self) -> List[str]:
        """Remove "thumbnail_data_url" field because it is computed field and it's not a field that we can request from Facebook"""
        return [f for f in super().fields if f != "thumbnail_data_url"]

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Read with super method and append thumbnail_data_url if enabled"""
        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            if self._fetch_thumbnail_images:
                thumbnail_url = record.get("thumbnail_url")
                if thumbnail_url:
                    record["thumbnail_data_url"] = fetch_thumbnail_data_url(thumbnail_url)
            yield record

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ad_creatives(params=params)


class CustomConversions(FBMarketingStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/custom-conversion"""

    entity_prefix = "customconversion"
    enable_deleted = False

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_custom_conversions(params=params)


class CustomAudiences(FBMarketingStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/custom-conversion"""

    entity_prefix = "customaudience"
    enable_deleted = False

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_custom_audiences(params=params)


class Ads(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup"""

    entity_prefix = "ad"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ads(params=params)


class AdSets(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign"""

    entity_prefix = "adset"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ad_sets(params=params)


class Campaigns(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group"""

    entity_prefix = "campaign"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_campaigns(params=params)


class Activities(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-activity"""

    entity_prefix = "activity"
    cursor_field = "event_time"
    primary_key = None

    def list_objects(self, fields: List[str], params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_activities(fields=fields, params=params)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Main read method used by CDK"""
        loaded_records_iter = self.list_objects(fields=self.fields, params=self.request_params(stream_state=stream_state))

        for record in loaded_records_iter:
            if isinstance(record, AbstractObject):
                yield record.export_all_data()  # convert FB object to dict
            else:
                yield record  # execute_in_batch will emmit dicts

    def _state_filter(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        """Additional filters associated with state if any set"""
        state_value = stream_state.get(self.cursor_field)
        since = self._start_date if not state_value else pendulum.parse(state_value)

        potentially_new_records_in_the_past = self._include_deleted and not stream_state.get("include_deleted", False)
        if potentially_new_records_in_the_past:
            self.logger.info(f"Ignoring bookmark for {self.name} because of enabled `include_deleted` option")
            since = self._start_date

        return {"since": since.int_timestamp}


class Videos(FBMarketingReversedIncrementalStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/video"""

    entity_prefix = "video"

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        # Remove filtering as it is not working for this stream since 2023-01-13
        return self._api.account.get_ad_videos(params=params, fields=self.fields)


class AdAccount(FBMarketingStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/ad-account"""

    use_batch = False
    enable_deleted = False

    def get_task_permissions(self) -> Set[str]:
        """https://developers.facebook.com/docs/marketing-api/reference/ad-account/assigned_users/"""
        res = set()
        me = User(fbid="me", api=self._api.api)
        for business_user in me.get_business_users():
            assigned_users = self._api.account.get_assigned_users(params={"business": business_user["business"].get_id()})
            for assigned_user in assigned_users:
                if business_user.get_id() == assigned_user.get_id():
                    res.update(set(assigned_user["tasks"]))
        return res

    @cached_property
    def fields(self) -> List[str]:
        properties = super().fields
        # https://developers.facebook.com/docs/marketing-apis/guides/javascript-ads-dialog-for-payments/
        # To access "funding_source_details", the user making the API call must have a MANAGE task permission for
        # that specific ad account.
        if "funding_source_details" in properties and "MANAGE" not in self.get_task_permissions():
            properties.remove("funding_source_details")
        if "is_prepay_account" in properties and "MANAGE" not in self.get_task_permissions():
            properties.remove("is_prepay_account")
        return properties

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        """noop in case of AdAccount"""
        return [FBAdAccount(self._api.account.get_id())]


class Images(FBMarketingReversedIncrementalStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/ad-image"""

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        return self._api.account.get_ad_images(params=params, fields=self.fields)

    def get_record_deleted_status(self, record) -> bool:
        return record[AdImage.Field.status] == AdImage.Status.deleted


class AdsInsightsAgeAndGender(AdsInsights):
    breakdowns = ["age", "gender"]


class AdsInsightsCountry(AdsInsights):
    breakdowns = ["country"]


class AdsInsightsRegion(AdsInsights):
    breakdowns = ["region"]


class AdsInsightsDma(AdsInsights):
    breakdowns = ["dma"]


class AdsInsightsPlatformAndDevice(AdsInsights):
    breakdowns = ["publisher_platform", "platform_position", "impression_device"]
    # FB Async Job fails for unknown reason if we set other breakdowns
    # my guess: it fails because of very large cardinality of result set (Eugene K)
    action_breakdowns = ["action_type"]


class AdsInsightsActionType(AdsInsights):
    breakdowns = []
    action_breakdowns = ["action_type"]


class AdsInsightsActionCarouselCard(AdsInsights):
    action_breakdowns = ["action_carousel_card_id", "action_carousel_card_name"]


class AdsInsightsActionConversionDevice(AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsActionProductID(AdsInsights):
    breakdowns = ["product_id"]
    action_breakdowns = []


class AdsInsightsActionReaction(AdsInsights):
    action_breakdowns = ["action_reaction"]


class AdsInsightsActionVideoSound(AdsInsights):
    action_breakdowns = ["action_video_sound"]


class AdsInsightsActionVideoType(AdsInsights):
    action_breakdowns = ["action_video_type"]


class AdsInsightsDeliveryDevice(AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsDeliveryPlatform(AdsInsights):
    breakdowns = ["publisher_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsDeliveryPlatformAndDevicePlatform(AdsInsights):
    breakdowns = ["publisher_platform", "device_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsAge(AdsInsights):
    breakdowns = ["age"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsCountry(AdsInsights):
    breakdowns = ["country"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsDMARegion(AdsInsights):
    breakdowns = ["dma"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsGender(AdsInsights):
    breakdowns = ["gender"]
    action_breakdowns = ["action_type"]


class AdsLeadsData(FBMarketingIncrementalStream):
    entity_prefix = "leads"
    filter_field = "time_created"
    cursor_field = "created_time"
    ad_id = "ad_id"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """customized read method for ads leads data"""
        ad_sets_records_iter = self._api.account.get_ads(fields={}, params={})
        ad_sets_loaded_records_iter = (ad_record.api_get(fields={}, pending=self.use_batch) for ad_record in ad_sets_records_iter)
        if self.use_batch:
            ad_sets_loaded_records_iter = self.execute_in_batch(ad_sets_loaded_records_iter)
        for ad_record in ad_sets_loaded_records_iter:
            # get lead for ad_id
            if ad_record.get("id"):
                logger.info(f"Running leads sync for Ad_ID[{ad_record['id']}]")
                records_iter = Ad(ad_record["id"]).get_leads(
                    fields=self.fields, params=self.request_params(ad_id=ad_record["id"], stream_state=stream_state)
                )
                loaded_records_iter = (record.api_get(fields=self.fields, pending=self.use_batch) for record in records_iter)
                if self.use_batch:
                    loaded_records_iter = self.execute_in_batch(loaded_records_iter)

                for record in loaded_records_iter:
                    if isinstance(record, AbstractObject):
                        yield record.export_all_data()  # convert FB object to dict
                    else:
                        yield record  # execute_in_batch will emmit dicts
                logger.info(f"Finished leads sync for Ad_ID[{ad_record['id']}]")

    def list_objects(self, params: Mapping[str, Any]) -> Iterable:
        """Because leads has very different read_records we don't need this method anymore"""

    def request_params(self, ad_id: str, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        """Include state filter"""
        return self._state_filter(ad_id=ad_id, stream_state=stream_state)

    def _state_filter(self, ad_id: str, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        """Additional filters associated with state if any set"""
        state_value = stream_state.get(ad_id)
        filter_value = self._start_date if not state_value else pendulum.parse(state_value)
        return {
            "filtering": [
                {
                    "field": f"{self.filter_field}",
                    "operator": "GREATER_THAN",
                    "value": filter_value.int_timestamp,
                },
            ],
        }

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        """
        Update stream state from latest record
        Example: {"ads_leads_data":{"100001956515190222": "2023-12-11T15:53:59+0000"}}
        """
        # get ad_id of current record
        record_ads_id = latest_record[self.ad_id]
        # get cursor field of current record
        record_cursor_field = latest_record[self.cursor_field]
        # get cursor field from state
        state_cursor_field = None
        if current_stream_state:
            state_cursor_field = current_stream_state.get(f"{record_ads_id}")

        if not current_stream_state:
            current_stream_state = {}
            current_stream_state[f"{record_ads_id}"] = record_cursor_field
        elif state_cursor_field is None or pendulum.parse(record_cursor_field) > pendulum.parse(state_cursor_field):
            current_stream_state[f"{record_ads_id}"] = record_cursor_field
        return current_stream_state
