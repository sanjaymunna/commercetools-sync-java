package com.commercetools.sync.inventories.helpers;


import com.commercetools.sync.commons.exceptions.ReferenceResolutionException;
import com.commercetools.sync.inventories.InventorySyncMockUtils;
import com.commercetools.sync.inventories.InventorySyncOptions;
import com.commercetools.sync.inventories.InventorySyncOptionsBuilder;
import com.commercetools.sync.services.ChannelService;
import com.commercetools.sync.services.TypeService;
import io.sphere.sdk.channels.Channel;
import io.sphere.sdk.client.SphereClient;
import io.sphere.sdk.inventory.InventoryEntryDraft;
import io.sphere.sdk.inventory.InventoryEntryDraftBuilder;
import io.sphere.sdk.models.Reference;
import io.sphere.sdk.models.SphereException;
import io.sphere.sdk.types.CustomFieldsDraft;
import io.sphere.sdk.utils.CompletableFutureUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.commercetools.sync.commons.MockUtils.getMockTypeService;
import static com.commercetools.sync.commons.helpers.BaseReferenceResolver.BLANK_ID_VALUE_ON_RESOURCE_IDENTIFIER;
import static com.commercetools.sync.commons.helpers.CustomReferenceResolver.TYPE_DOES_NOT_EXIST;
import static com.commercetools.sync.inventories.InventorySyncMockUtils.getMockSupplyChannel;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InventoryReferenceResolverTest {
    private TypeService typeService;
    private ChannelService channelService;
    private InventorySyncOptions syncOptions;

    private static final String SKU = "1000";
    private static final String CHANNEL_KEY = "channel-key_1";
    private static final String UUID_KEY = UUID.randomUUID().toString();
    private static final String CUSTOM_TYPE_KEY = "customType-key_1";
    private static final String CHANNEL_ID = "1";
    private static final Long QUANTITY = 10L;
    private static final Integer RESTOCKABLE_IN_DAYS = 10;
    private static final ZonedDateTime DATE_1 = ZonedDateTime.of(2017, 4, 1, 10, 0, 0, 0, ZoneId.of("UTC"));

    /**
     * Sets up the services and the options needed for reference resolution.
     */
    @Before
    public void setup() {
        typeService = getMockTypeService();
        channelService = InventorySyncMockUtils.getMockChannelService(getMockSupplyChannel(CHANNEL_ID, CHANNEL_KEY));
        syncOptions = InventorySyncOptionsBuilder.of(mock(SphereClient.class)).build();
    }

    @Test
    public void
        resolveSupplyChannelReference_WithNonExistingChannelAndNotEnsureChannel_ShouldNotResolveChannelReference() {
        // preparation
        when(channelService.fetchCachedChannelId(anyString()))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        final InventoryEntryDraft draft = InventoryEntryDraft
            .of(SKU, QUANTITY, DATE_1, RESTOCKABLE_IN_DAYS, Channel.referenceOfId(CHANNEL_KEY))
            .withCustom(CustomFieldsDraft.ofTypeIdAndJson(CUSTOM_TYPE_KEY, new HashMap<>()));

        final InventoryReferenceResolver referenceResolver =
            new InventoryReferenceResolver(syncOptions, typeService, channelService);

        // assertion and test
        assertThat(referenceResolver.resolveSupplyChannelReference(InventoryEntryDraftBuilder.of(draft)))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasCauseInstanceOf(CompletionException.class)
            .hasRootCauseExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessageContaining("Channel with key 'channel-key_1' does not exist.");
    }

    @Test
    public void
        resolveSupplyChannelReference_WithNonExistingChannelAndEnsureChannel_ShouldResolveSupplyChannelReference() {
        // preparation
        final InventorySyncOptions optionsWithEnsureChannels = InventorySyncOptionsBuilder.of(mock(SphereClient.class)).ensureChannels(true)
                                                                                          .build();
        when(channelService.fetchCachedChannelId(anyString()))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        final InventoryEntryDraft draft = InventoryEntryDraft
            .of(SKU, QUANTITY, DATE_1, RESTOCKABLE_IN_DAYS, Channel.referenceOfId(CHANNEL_KEY))
            .withCustom(CustomFieldsDraft.ofTypeIdAndJson(CUSTOM_TYPE_KEY, new HashMap<>()));

        final InventoryReferenceResolver referenceResolver =
            new InventoryReferenceResolver(optionsWithEnsureChannels, typeService, channelService);

        final InventoryEntryDraftBuilder expectedDraftBuilder =
            InventoryEntryDraftBuilder.of(draft).supplyChannel(Channel.referenceOfId(CHANNEL_ID));

        // assertion and test
        assertThat(referenceResolver.resolveSupplyChannelReference(InventoryEntryDraftBuilder.of(draft)))
            .isCompletedWithValue(expectedDraftBuilder);
    }

    @Test
    public void resolveCustomTypeReference_WithExceptionOnCustomTypeFetch_ShouldNotResolveReferences() {
        // preparation
        final InventoryEntryDraftBuilder draftBuilder = InventoryEntryDraftBuilder
            .of(SKU, QUANTITY, DATE_1, RESTOCKABLE_IN_DAYS, Channel.referenceOfId(UUID_KEY))
            .custom(CustomFieldsDraft.ofTypeIdAndJson(CUSTOM_TYPE_KEY, new HashMap<>()));

        final SphereException sphereException = new SphereException("bad request");
        when(typeService.fetchCachedTypeId(anyString()))
            .thenReturn(CompletableFutureUtils.failed(sphereException));

        final InventoryReferenceResolver referenceResolver =
            new InventoryReferenceResolver(syncOptions, typeService, channelService);

        // assertion and test
        assertThat(referenceResolver.resolveCustomTypeReference(draftBuilder))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(SphereException.class)
            .hasMessage(sphereException.getMessage());
    }

    @Test
    public void resolveCustomTypeReference_WithNonExistentCustomType_ShouldNotResolveCustomTypeReference() {
        // preparation
        when(typeService.fetchCachedTypeId(anyString()))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        final InventoryEntryDraftBuilder draftBuilder = InventoryEntryDraftBuilder
            .of(SKU, QUANTITY, DATE_1, RESTOCKABLE_IN_DAYS, Channel.referenceOfId(CHANNEL_KEY))
            .custom(CustomFieldsDraft.ofTypeIdAndJson(CUSTOM_TYPE_KEY, new HashMap<>()));

        final InventoryReferenceResolver referenceResolver =
            new InventoryReferenceResolver(syncOptions, typeService, channelService);

        // assertion and test
        assertThat(referenceResolver.resolveCustomTypeReference(draftBuilder))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(format(TYPE_DOES_NOT_EXIST, CUSTOM_TYPE_KEY));
    }

    @Test
    public void resolveSupplyChannelReference_WithEmptyIdOnSupplyChannelReference_ShouldNotResolveChannelReference() {
        // preparation
        final InventoryEntryDraft draft = InventoryEntryDraft
            .of(SKU, QUANTITY, DATE_1, RESTOCKABLE_IN_DAYS, Channel.referenceOfId(""))
            .withCustom(CustomFieldsDraft.ofTypeIdAndJson(CUSTOM_TYPE_KEY, new HashMap<>()));

        final InventoryReferenceResolver referenceResolver =
            new InventoryReferenceResolver(syncOptions, typeService, channelService);

        // assertion and test
        assertThat(referenceResolver.resolveSupplyChannelReference(InventoryEntryDraftBuilder.of(draft)))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(format("Failed to resolve supply channel reference on InventoryEntryDraft"
                + " with SKU:'%s'. Reason: %s", SKU, BLANK_ID_VALUE_ON_RESOURCE_IDENTIFIER));
    }

    @Test
    public void resolveSupplyChannelReference_WithNullIdOnChannelReference_ShouldNotResolveSupplyChannelReference() {
        // preparation
        final InventoryEntryDraft draft = mock(InventoryEntryDraft.class);
        final Reference<Channel> supplyChannelReference = Channel.referenceOfId(null);
        when(draft.getSupplyChannel()).thenReturn(supplyChannelReference);

        final InventoryReferenceResolver referenceResolver =
            new InventoryReferenceResolver(syncOptions, typeService, channelService);

        // assertion and test
        assertThat(referenceResolver.resolveSupplyChannelReference(InventoryEntryDraftBuilder.of(draft)))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(format("Failed to resolve supply channel reference on InventoryEntryDraft"
                + " with SKU:'null'. Reason: %s", BLANK_ID_VALUE_ON_RESOURCE_IDENTIFIER));
    }

    @Test
    public void resolveCustomTypeReference_WithEmptyIdOnCustomTypeReference_ShouldNotResolveCustomTypeReference() {
        // preparation
        final InventoryEntryDraftBuilder draftBuilder = InventoryEntryDraftBuilder
            .of(SKU, QUANTITY, DATE_1, RESTOCKABLE_IN_DAYS, Channel.referenceOfId(CHANNEL_KEY))
            .custom(CustomFieldsDraft.ofTypeIdAndJson("", new HashMap<>()));

        final InventoryReferenceResolver referenceResolver =
            new InventoryReferenceResolver(syncOptions, typeService, channelService);

        // assertion and test
        assertThat(referenceResolver.resolveCustomTypeReference(draftBuilder))
            .hasFailedWithThrowableThat()
            .isExactlyInstanceOf(ReferenceResolutionException.class)
            .hasMessage(format("Failed to resolve custom type reference on InventoryEntryDraft"
                + " with SKU:'1000'. Reason: %s", BLANK_ID_VALUE_ON_RESOURCE_IDENTIFIER));
    }
}
