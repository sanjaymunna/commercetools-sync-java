package com.commercetools.sync.integration.ctpprojectsource.cartdiscounts;

import com.commercetools.sync.cartdiscounts.CartDiscountSync;
import com.commercetools.sync.cartdiscounts.CartDiscountSyncOptions;
import com.commercetools.sync.cartdiscounts.CartDiscountSyncOptionsBuilder;
import com.commercetools.sync.cartdiscounts.helpers.CartDiscountSyncStatistics;
import io.sphere.sdk.cartdiscounts.AbsoluteCartDiscountValue;
import io.sphere.sdk.cartdiscounts.CartDiscount;
import io.sphere.sdk.cartdiscounts.CartDiscountDraft;
import io.sphere.sdk.cartdiscounts.CartDiscountDraftBuilder;
import io.sphere.sdk.cartdiscounts.CartDiscountDraftDsl;
import io.sphere.sdk.cartdiscounts.CartDiscountValue;
import io.sphere.sdk.cartdiscounts.CartPredicate;
import io.sphere.sdk.cartdiscounts.GiftLineItemCartDiscountValue;
import io.sphere.sdk.cartdiscounts.ShippingCostTarget;
import io.sphere.sdk.cartdiscounts.commands.CartDiscountCreateCommand;
import io.sphere.sdk.cartdiscounts.commands.updateactions.ChangeCartPredicate;
import io.sphere.sdk.cartdiscounts.commands.updateactions.ChangeTarget;
import io.sphere.sdk.cartdiscounts.commands.updateactions.ChangeValue;
import io.sphere.sdk.commands.UpdateAction;
import io.sphere.sdk.models.ResourceIdentifier;
import io.sphere.sdk.products.Product;
import io.sphere.sdk.products.queries.ProductByKeyGet;
import io.sphere.sdk.utils.MoneyImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.commercetools.sync.cartdiscounts.utils.CartDiscountReferenceReplacementUtils.buildCartDiscountQuery;
import static com.commercetools.sync.cartdiscounts.utils.CartDiscountReferenceReplacementUtils.replaceCartDiscountsReferenceIdsWithKeys;
import static com.commercetools.sync.commons.asserts.statistics.AssertionsForStatistics.assertThat;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.CART_DISCOUNT_CART_PREDICATE_1;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.CART_DISCOUNT_DESC_1;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.CART_DISCOUNT_GIFT_LINEITEM_PRODUCT_KEY;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.CART_DISCOUNT_NAME_1;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.JANUARY_FROM;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.JANUARY_UNTIL;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.deleteCartDiscountsFromTargetAndSource;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.populateSourceProject;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.populateTargetProject;
import static com.commercetools.sync.integration.commons.utils.CartDiscountITUtils.getCustomFieldsDraft;
import static com.commercetools.sync.integration.commons.utils.ProductITUtils.deleteProductSyncTestData;
import static com.commercetools.sync.integration.commons.utils.SphereClientUtils.CTP_SOURCE_CLIENT;
import static com.commercetools.sync.integration.commons.utils.SphereClientUtils.CTP_TARGET_CLIENT;
import static io.sphere.sdk.models.DefaultCurrencyUnits.EUR;
import static org.assertj.core.api.Assertions.assertThat;

class CartDiscountSyncIT {

    @BeforeEach
    void setup() {
        deleteCartDiscountsFromTargetAndSource();
        deleteProductSyncTestData(CTP_SOURCE_CLIENT);
        deleteProductSyncTestData(CTP_TARGET_CLIENT);
        populateSourceProject();
        populateTargetProject();
    }

    @AfterAll
    static void tearDown() {
        deleteCartDiscountsFromTargetAndSource();
        deleteProductSyncTestData(CTP_SOURCE_CLIENT);
        deleteProductSyncTestData(CTP_TARGET_CLIENT);
    }

    @Test
    void sync_WithoutUpdates_ShouldReturnProperStatistics() {
        // preparation
        final List<CartDiscount> cartDiscounts = CTP_SOURCE_CLIENT
            .execute(buildCartDiscountQuery())
            .toCompletableFuture().join().getResults();

        final List<CartDiscountDraft> cartDiscountDrafts = replaceCartDiscountsReferenceIdsWithKeys(cartDiscounts);

        final List<String> errorMessages = new ArrayList<>();
        final List<Throwable> exceptions = new ArrayList<>();

        final CartDiscountSyncOptions cartDiscountSyncOptions = CartDiscountSyncOptionsBuilder
            .of(CTP_TARGET_CLIENT)
            .errorCallback((error, throwable) -> {
                errorMessages.add(error);
                exceptions.add(throwable);
            })
            .build();

        final CartDiscountSync cartDiscountSync = new CartDiscountSync(cartDiscountSyncOptions);

        // test
        final CartDiscountSyncStatistics cartDiscountSyncStatistics = cartDiscountSync
            .sync(cartDiscountDrafts)
            .toCompletableFuture().join();

        // assertion
        assertThat(errorMessages).isEmpty();
        assertThat(exceptions).isEmpty();
        assertThat(cartDiscountSyncStatistics).hasValues(3, 2, 0, 0);
        assertThat(cartDiscountSyncStatistics
            .getReportMessage())
            .isEqualTo("Summary: 3 cart discounts were processed in total"
                + " (2 created, 0 updated and 0 failed to sync).");

    }

    @Test
    void sync_WithUpdates_ShouldReturnProperStatistics() {
        // preparation
        final List<CartDiscount> cartDiscounts = CTP_SOURCE_CLIENT
            .execute(buildCartDiscountQuery())
            .toCompletableFuture().join().getResults();

        final List<CartDiscountDraft> cartDiscountsReferenceIdsWithKeys =
            replaceCartDiscountsReferenceIdsWithKeys(cartDiscounts);

        // Apply some changes
        final List<CartDiscountDraft> cartDiscountDrafts = cartDiscountsReferenceIdsWithKeys
            .stream()
            .map(draft -> CartDiscountDraftBuilder
                .of(draft)
                .cartPredicate(CartPredicate.of("totalPrice >= \"100 EUR\""))
                .value(AbsoluteCartDiscountValue.of(MoneyImpl.of(40, EUR)))
                .target(ShippingCostTarget.of())
                .build())
            .collect(Collectors.toList());

        final List<String> errorMessages = new ArrayList<>();
        final List<Throwable> exceptions = new ArrayList<>();
        final List<UpdateAction<CartDiscount>> updateActionsList = new ArrayList<>();

        final CartDiscountSyncOptions cartDiscountSyncOptions = CartDiscountSyncOptionsBuilder
            .of(CTP_TARGET_CLIENT)
            .errorCallback((error, throwable) -> {
                errorMessages.add(error);
                exceptions.add(throwable);
            })
            .beforeUpdateCallback((updateActions, newCartDiscount, oldCartDiscount) -> {
                updateActionsList.addAll(updateActions);
                return updateActions;
            })
            .build();

        final CartDiscountSync cartDiscountSync = new CartDiscountSync(cartDiscountSyncOptions);

        // test
        final CartDiscountSyncStatistics cartDiscountSyncStatistics = cartDiscountSync
            .sync(cartDiscountDrafts)
            .toCompletableFuture().join();

        // assertion
        assertThat(errorMessages).isEmpty();
        assertThat(exceptions).isEmpty();
        assertThat(updateActionsList).containsExactly(
                ChangeValue.of(CartDiscountValue.ofAbsolute(Collections.singletonList(MoneyImpl.of(40, EUR)))),
                ChangeCartPredicate.of("totalPrice >= \"100 EUR\""),
                ChangeTarget.of(ShippingCostTarget.of())
        );
        assertThat(cartDiscountSyncStatistics).hasValues(2, 1, 1, 0);
        assertThat(cartDiscountSyncStatistics
            .getReportMessage())
            .isEqualTo("Summary: 2 cart discounts were processed in total"
                + " (1 created, 1 updated and 0 failed to sync).");
    }

    @Test
    void sync_WithProductReferenceWithIdNotKey_ShouldFailToResolve() {
        // preparation
        final Product sourceProduct = CTP_SOURCE_CLIENT
            .execute(ProductByKeyGet.of(CART_DISCOUNT_GIFT_LINEITEM_PRODUCT_KEY))
            .toCompletableFuture()
            .join();

        final GiftLineItemCartDiscountValue inValidValue = GiftLineItemCartDiscountValue.of(
            ResourceIdentifier.ofId(sourceProduct.getId()), 1, null, null);
        final CartDiscountDraftDsl invalidDraft =
            CartDiscountDraftBuilder.of(CART_DISCOUNT_NAME_1,
                CART_DISCOUNT_CART_PREDICATE_1,
                inValidValue,
                null,
                "0.2439849",
                false)
                                    .key("invalidCartDiscountKey")
                                    .active(false)
                                    .description(CART_DISCOUNT_DESC_1)
                                    .validFrom(JANUARY_FROM)
                                    .validUntil(JANUARY_UNTIL)
                                    .custom(getCustomFieldsDraft())
                                    .build();

        CTP_SOURCE_CLIENT.execute(CartDiscountCreateCommand.of(invalidDraft)).toCompletableFuture().join();

        final List<CartDiscount> cartDiscounts = CTP_SOURCE_CLIENT
            .execute(buildCartDiscountQuery())
            .toCompletableFuture()
            .join()
            .getResults();


        final List<CartDiscountDraft> cartDiscountDrafts =
            replaceCartDiscountsReferenceIdsWithKeys(cartDiscounts);

        final List<String> errorMessages = new ArrayList<>();
        final List<Throwable> exceptions = new ArrayList<>();
        final List<UpdateAction<CartDiscount>> updateActionsList = new ArrayList<>();

        final CartDiscountSyncOptions cartDiscountSyncOptions = CartDiscountSyncOptionsBuilder
            .of(CTP_TARGET_CLIENT)
            .errorCallback((error, throwable) -> {
                errorMessages.add(error);
                exceptions.add(throwable);
            })
            .beforeUpdateCallback((updateActions, newCartDiscount, oldCartDiscount) -> {
                updateActionsList.addAll(updateActions);
                return updateActions;
            })
            .build();

        final CartDiscountSync cartDiscountSync = new CartDiscountSync(cartDiscountSyncOptions);

        // test
        final CartDiscountSyncStatistics cartDiscountSyncStatistics = cartDiscountSync
            .sync(cartDiscountDrafts)
            .toCompletableFuture().join();

        // assertion
        assertThat(errorMessages).isEmpty();
        assertThat(exceptions).isEmpty();
        assertThat(updateActionsList).containsExactly(
            ChangeValue.of(CartDiscountValue.ofAbsolute(Collections.singletonList(MoneyImpl.of(40, EUR)))),
            ChangeCartPredicate.of("totalPrice >= \"100 EUR\""),
            ChangeTarget.of(ShippingCostTarget.of())
        );
        assertThat(cartDiscountSyncStatistics).hasValues(2, 1, 1, 0);
        assertThat(cartDiscountSyncStatistics
            .getReportMessage())
            .isEqualTo("Summary: 2 cart discounts were processed in total"
                + " (1 created, 1 updated and 0 failed to sync).");
    }

}